﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow;
using Sparrow.Logging;
using Voron.Impl;
using System.Linq;
using Lucene.Net.Store;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexFacetedReadOperation : IndexOperationBase
    {
        private readonly IndexSearcher _searcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IndexSearcherHolder.IndexSearcherHoldingState _currentStateHolder;

        private readonly IState _state;

        public IndexFacetedReadOperation(Index index,
            Dictionary<string, IndexField> fields,
            LuceneVoronDirectory directory,
            IndexSearcherHolder searcherHolder,
            Transaction readTransaction,
            DocumentDatabase documentDatabase)
            : base(index, LoggingSource.Instance.GetLogger<IndexFacetedReadOperation>(documentDatabase.Name))
        {
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _currentStateHolder = searcherHolder.GetStateHolder(readTransaction);
            _searcher = _currentStateHolder.GetIndexSearcher(_state);
        }

        public Dictionary<string, FacetResult> FacetedQuery(FacetQueryServerSide query, JsonOperationContext context, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            var results = FacetedQueryParser.Parse(query.Facets, out Dictionary<string, Facet> defaultFacets, out Dictionary<string, List<FacetedQueryParser.ParsedRange>> rangeFacets);

            var facetsByName = new Dictionary<string, Dictionary<string, FacetValue>>();

            uint fieldsHash = 0;
            if (query.Metadata.IsDistinct)
                fieldsHash = CalculateQueryFieldsHash(query);

            var baseQuery = GetLuceneQuery(context, query.Metadata, query.QueryParameters, _analyzer, getSpatialField);
            var returnedReaders = GetQueryMatchingDocuments(_searcher, baseQuery, _state);

            foreach (var facet in defaultFacets.Values)
            {
                if (facet.Mode != FacetMode.Default)
                    continue;

                Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>> distinctItems = null;
                HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen = null;
                if (query.Metadata.IsDistinct)
                    distinctItems = new Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>>();

                foreach (var readerFacetInfo in returnedReaders)
                {
                    var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, facet.Name, _indexName, _state);


                    if (facetsByName.TryGetValue(facet.DisplayName, out Dictionary<string, FacetValue> facetValues) == false)
                    {
                        facetsByName[facet.DisplayName] = facetValues = new Dictionary<string, FacetValue>();
                    }

                    foreach (var kvp in termsForField)
                    {
                        if (query.Metadata.IsDistinct)
                        {
                            if (distinctItems.TryGetValue(kvp.Key, out alreadySeen) == false)
                            {
                                alreadySeen = new HashSet<IndexSearcherHolder.StringCollectionValue>();
                                distinctItems[kvp.Key] = alreadySeen;
                            }
                        }

                        var needToApplyAggregation = (facet.Aggregation == FacetAggregation.None || facet.Aggregation == FacetAggregation.Count) == false;
                        var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, alreadySeen, query, fieldsHash, needToApplyAggregation, context);
                        var intersectCount = intersectedDocuments.Count;
                        if (intersectCount == 0)
                            continue;

                        if (facetValues.TryGetValue(kvp.Key, out FacetValue facetValue) == false)
                        {
                            facetValue = new FacetValue
                            {
                                Range = FacetedQueryHelper.GetRangeName(facet.Name, kvp.Key)
                            };
                            facetValues.Add(kvp.Key, facetValue);
                        }
                        facetValue.Hits += intersectCount;
                        facetValue.Count = facetValue.Hits;

                        if (needToApplyAggregation)
                        {
                            var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                            ApplyAggregation(facet, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);
                        }
                    }
                }
            }

            foreach (var range in rangeFacets)
            {
                var facet = defaultFacets[range.Key];
                var needToApplyAggregation = (facet.Aggregation == FacetAggregation.None || facet.Aggregation == FacetAggregation.Count) == false;

                Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>> distinctItems = null;
                HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen = null;
                if (query.Metadata.IsDistinct)
                    distinctItems = new Dictionary<string, HashSet<IndexSearcherHolder.StringCollectionValue>>();

                foreach (var readerFacetInfo in returnedReaders)
                {
                    var termsForField = IndexedTerms.GetTermsAndDocumentsFor(readerFacetInfo.Reader, readerFacetInfo.DocBase, facet.Name, _indexName, _state);
                    if (query.Metadata.IsDistinct)
                    {
                        if (distinctItems.TryGetValue(range.Key, out alreadySeen) == false)
                        {
                            alreadySeen = new HashSet<IndexSearcherHolder.StringCollectionValue>();
                            distinctItems[range.Key] = alreadySeen;
                        }
                    }

                    var facetResult = results[range.Key];
                    var ranges = range.Value;
                    foreach (var kvp in termsForField)
                    {
                        for (int i = 0; i < ranges.Count; i++)
                        {
                            var parsedRange = ranges[i];
                            if (parsedRange.IsMatch(kvp.Key))
                            {
                                var facetValue = facetResult.Values[i];

                                var intersectedDocuments = GetIntersectedDocuments(new ArraySegment<int>(kvp.Value), readerFacetInfo.Results, alreadySeen, query, fieldsHash, needToApplyAggregation, context);
                                var intersectCount = intersectedDocuments.Count;
                                if (intersectCount == 0)
                                    continue;

                                facetValue.Hits += intersectCount;
                                facetValue.Count = facetValue.Hits;

                                if (needToApplyAggregation)
                                {
                                    var docsInQuery = new ArraySegment<int>(intersectedDocuments.Documents, 0, intersectedDocuments.Count);
                                    ApplyAggregation(facet, facetValue, docsInQuery, readerFacetInfo.Reader, readerFacetInfo.DocBase, _state);
                                    IntArraysPool.Instance.FreeArray(intersectedDocuments.Documents);
                                    intersectedDocuments.Documents = null;
                                }
                            }
                        }
                    }
                }
            }

            UpdateFacetResults(results, query, defaultFacets, facetsByName);

            CompleteFacetCalculationsStage(results, defaultFacets);

            foreach (var readerFacetInfo in returnedReaders)
            {
                IntArraysPool.Instance.FreeArray(readerFacetInfo.Results.Array);
            }

            return results;
        }

        private static unsafe uint CalculateQueryFieldsHash(FacetQueryServerSide query)
        {
            uint hash = 0;

            foreach (var field in query.Metadata.SelectFields)
            {
                fixed (char* p = field.Name.Value)
                {
                    hash = Hashing.XXHash32.Calculate((byte*)p, sizeof(char) * field.Name.Value.Length, hash);
                }
            }

            return hash;
        }

        private static void UpdateFacetResults(Dictionary<string, FacetResult> results, FacetQueryServerSide query, Dictionary<string, Facet> facets, Dictionary<string, Dictionary<string, FacetValue>> facetsByName)
        {
            foreach (var facet in facets.Values)
            {
                if (facet.Mode == FacetMode.Ranges)
                    continue;

                var values = new List<FacetValue>();
                List<string> allTerms;
                if (facetsByName.TryGetValue(facet.DisplayName, out Dictionary<string, FacetValue> groups) == false || groups == null)
                    continue;

                switch (facet.TermSortMode)
                {
                    case FacetTermSortMode.ValueAsc:
                        allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.ValueDesc:
                        allTerms = new List<string>(groups.OrderByDescending(x => x.Key).ThenBy(x => x.Value.Hits).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.HitsAsc:
                        allTerms = new List<string>(groups.OrderBy(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
                        break;
                    case FacetTermSortMode.HitsDesc:
                        allTerms = new List<string>(groups.OrderByDescending(x => x.Value.Hits).ThenBy(x => x.Key).Select(x => x.Key));
                        break;
                    default:
                        throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
                }

                var pageSize = Math.Min(allTerms.Count, query.PageSize);
                int maxResults = facet.MaxResults.HasValue ? Math.Min(pageSize, facet.MaxResults.Value) : pageSize;

                foreach (var term in allTerms.Skip(query.Start).TakeWhile(term => values.Count < maxResults))
                {
                    if (groups.TryGetValue(term, out FacetValue facetValue) == false || facetValue == null)
                        facetValue = new FacetValue { Range = term };

                    values.Add(facetValue);
                }

                var previousHits = allTerms.Take(query.Start).Sum(allTerm =>
                {
                    if (groups.TryGetValue(allTerm, out FacetValue facetValue) == false || facetValue == null)
                        return 0;

                    return facetValue.Hits;
                });

                var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

                results[key] = new FacetResult
                {
                    Values = values,
                    RemainingTermsCount = allTerms.Count - (query.Start + values.Count),
                    RemainingHits = groups.Values.Sum(x => x.Hits) - (previousHits + values.Sum(x => x.Hits))
                };

                if (facet.IncludeRemainingTerms)
                    results[key].RemainingTerms = allTerms.Skip(query.Start + values.Count).ToList();
            }
        }

        private static void CompleteFacetCalculationsStage(Dictionary<string, FacetResult> results, Dictionary<string, Facet> facets)
        {
            foreach (var facetResult in results)
            {
                var key = facetResult.Key;
                foreach (var facet in facets.Values.Where(f => f.DisplayName == key))
                {
                    if ((facet.Aggregation & FacetAggregation.Average) == FacetAggregation.Average)
                    {
                        foreach (var facetValue in facetResult.Value.Values)
                        {
                            if (facetValue.Hits == 0)
                                facetValue.Average = double.NaN;
                            else
                                facetValue.Average = facetValue.Average / facetValue.Hits;
                        }
                    }
                }
            }
        }

        private static void ApplyAggregation(Facet facet, FacetValue value, ArraySegment<int> docsInQuery, IndexReader indexReader, int docBase, IState state)
        {
            var name = facet.AggregationField;
            var rangeType = FieldUtil.GetRangeTypeFromFieldName(name);
            if (rangeType == RangeType.None)
            {
                name = FieldUtil.ApplyRangeSuffixIfNecessary(facet.AggregationField, RangeType.Double);
                rangeType = RangeType.Double;
            }

            long[] longs = null;
            double[] doubles = null;
            switch (rangeType)
            {
                case RangeType.Long:
                    longs = FieldCache_Fields.DEFAULT.GetLongs(indexReader, name, state);
                    break;
                case RangeType.Double:
                    doubles = FieldCache_Fields.DEFAULT.GetDoubles(indexReader, name, state);
                    break;
                default:
                    throw new InvalidOperationException("Invalid range type for " + facet.Name + ", don't know how to handle " + rangeType);
            }

            for (int index = 0; index < docsInQuery.Count; index++)
            {
                var doc = docsInQuery.Array[index];

                var currentVal = rangeType == RangeType.Long ? longs[doc - docBase] : doubles[doc - docBase];
                if ((facet.Aggregation & FacetAggregation.Max) == FacetAggregation.Max)
                {
                    value.Max = Math.Max(value.Max ?? double.MinValue, currentVal);
                }

                if ((facet.Aggregation & FacetAggregation.Min) == FacetAggregation.Min)
                {
                    value.Min = Math.Min(value.Min ?? double.MaxValue, currentVal);
                }

                if ((facet.Aggregation & FacetAggregation.Sum) == FacetAggregation.Sum)
                {
                    value.Sum = currentVal + (value.Sum ?? 0d);
                }

                if ((facet.Aggregation & FacetAggregation.Average) == FacetAggregation.Average)
                {
                    value.Average = currentVal + (value.Average ?? 0d);
                }
            }
        }

        private static List<ReaderFacetInfo> GetQueryMatchingDocuments(IndexSearcher currentIndexSearcher, Query baseQuery, IState state)
        {
            var gatherAllCollector = new GatherAllCollectorByReader();
            currentIndexSearcher.Search(baseQuery, gatherAllCollector, state);

            foreach (var readerFacetInfo in gatherAllCollector.Results)
            {
                readerFacetInfo.Complete();
            }

            return gatherAllCollector.Results;
        }

        /// <summary>
        /// This method expects both lists to be sorted
        /// </summary>
        private IntersectDocs GetIntersectedDocuments(ArraySegment<int> a, ArraySegment<int> b, HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen, FacetQueryServerSide query, uint fieldsHash, bool needToApplyAggregation, JsonOperationContext context)
        {
            ArraySegment<int> n, m;
            if (a.Count > b.Count)
            {
                n = a;
                m = b;
            }
            else
            {
                n = b;
                m = a;
            }

            int nSize = n.Count;
            int mSize = m.Count;

            double o1 = nSize + mSize;
            double o2 = nSize * Math.Log(mSize, 2);

            var isDistinct = query.Metadata.IsDistinct;
            var result = new IntersectDocs();
            if (needToApplyAggregation)
            {
                result.Documents = IntArraysPool.Instance.AllocateArray();
            }

            if (o1 < o2)
            {
                int mi = m.Offset, ni = n.Offset;
                while (mi < mSize && ni < nSize)
                {
                    var nVal = n.Array[ni];
                    var mVal = m.Array[mi];

                    if (nVal > mVal)
                    {
                        mi++;
                    }
                    else if (nVal < mVal)
                    {
                        ni++;
                    }
                    else
                    {
                        int docId = nVal;
                        if (isDistinct == false || IsDistinctValue(docId, alreadySeen, query, fieldsHash, context))
                        {
                            result.AddIntersection(docId);
                        }

                        ni++;
                        mi++;
                    }
                }
            }
            else
            {
                for (int i = m.Offset; i < mSize; i++)
                {
                    int docId = m.Array[i];
                    if (Array.BinarySearch(n.Array, n.Offset, n.Count, docId) >= 0)
                    {
                        if (isDistinct == false || IsDistinctValue(docId, alreadySeen, query, fieldsHash, context))
                        {
                            result.AddIntersection(docId);
                        }
                    }
                }
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsDistinctValue(int docId, HashSet<IndexSearcherHolder.StringCollectionValue> alreadySeen, FacetQueryServerSide query, uint fieldsHash, JsonOperationContext context)
        {
            var fields = _currentStateHolder.GetFieldsValues(docId, fieldsHash, query.Metadata.SelectFields, context, _state);
            return alreadySeen.Add(fields);
        }

        public override void Dispose()
        {
            _currentStateHolder?.Dispose();
            _releaseReadTransaction?.Dispose();
        }

        private class IntersectDocs
        {
            public int Count;
            public int[] Documents;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void AddIntersection(int docId)
            {
                if (Documents != null)
                {
                    if (Count >= Documents.Length)
                    {
                        IncreaseSize();
                    }
                    Documents[Count] = docId;
                }
                Count++;
            }

            private void IncreaseSize()
            {
                var newDocumentsArray = IntArraysPool.Instance.AllocateArray(Count * 2);
                Array.Copy(Documents, newDocumentsArray, Count);
                IntArraysPool.Instance.FreeArray(Documents);
                Documents = newDocumentsArray;
            }
        }

        private sealed class IntArraysPool : ILowMemoryHandler
        {
            public static readonly IntArraysPool Instance = new IntArraysPool();

            private readonly ConcurrentDictionary<int, ObjectPool<int[]>> _arraysPoolBySize = new ConcurrentDictionary<int, ObjectPool<int[]>>();
            //private readonly TimeSensitiveStore<ObjectPool<int[]>> _timeSensitiveStore = new TimeSensitiveStore<ObjectPool<int[]>>(TimeSpan.FromDays(1));

            private IntArraysPool()
            {
            }

            public int[] AllocateArray(int arraySize = 1024)
            {
                var roundedSize = GetRoundedSize(arraySize);
                var matchingQueue = _arraysPoolBySize.GetOrAdd(roundedSize, x => new ObjectPool<int[]>(() => new int[roundedSize]));

                var allocatedArray = matchingQueue.Allocate();

                //_timeSensitiveStore.Seen(matchingQueue);

                return allocatedArray;
            }

            public void FreeArray(int[] returnedArray)
            {
                if (returnedArray.Length != GetRoundedSize(returnedArray.Length))
                {
                    throw new ArgumentException("Array size does not match current array size constraints");
                }


                var matchingQueue = _arraysPoolBySize.GetOrAdd(returnedArray.Length, x => new ObjectPool<int[]>(() => new int[returnedArray.Length]));
                matchingQueue.Free(returnedArray);
            }

            private static int GetRoundedSize(int size)
            {
                const int roundSize = 1024;
                if (size % roundSize == 0)
                {
                    return size;
                }

                return (size / roundSize + 1) * roundSize;
            }

            private static void RunIdleOperations()
            {
                //_timeSensitiveStore.ForAllExpired(x =>
                //{
                //    var matchingQueue = _arraysPoolBySize.FirstOrDefault(y => y.Value == x).Key;
                //    if (matchingQueue != 0)
                //    {
                //        ObjectPool<int[]> removedQueue;
                //        _arraysPoolBySize.TryRemove(matchingQueue, out removedQueue);
                //    }
                //});
            }

            public void LowMemory()
            {
                RunIdleOperations();
            }

            public void LowMemoryOver()
            {
            }
        }

        private class ReaderFacetInfo
        {
            public IndexReader Reader;
            public int DocBase;
            // Here we store the _global document id_, if you need the 
            // reader document id, you must decrement with the DocBase
            private readonly LinkedList<int[]> _matches;
            private int[] _current;
            private int _pos;
            public ArraySegment<int> Results;

            public ReaderFacetInfo()
            {
                _current = IntArraysPool.Instance.AllocateArray();
                _matches = new LinkedList<int[]>();
            }

            public void AddMatch(int doc)
            {
                if (_pos >= _current.Length)
                {
                    _matches.AddLast(_current);
                    _current = IntArraysPool.Instance.AllocateArray();
                    _pos = 0;
                }
                _current[_pos++] = doc + DocBase;
            }

            public void Complete()
            {
                var size = _pos;
                foreach (var match in _matches)
                {
                    size += match.Length;
                }

                var mergedAndSortedArray = IntArraysPool.Instance.AllocateArray(size);
                var curMergedArrayIndex = 0;
                foreach (var match in _matches)
                {
                    Array.Copy(match, 0, mergedAndSortedArray, curMergedArrayIndex, match.Length);
                    curMergedArrayIndex += match.Length;
                    IntArraysPool.Instance.FreeArray(match);
                }

                Array.Copy(_current, 0, mergedAndSortedArray, curMergedArrayIndex, _pos);
                IntArraysPool.Instance.FreeArray(_current);
                curMergedArrayIndex += _pos;
                _current = null;
                _pos = 0;

                Array.Sort(mergedAndSortedArray, 0, curMergedArrayIndex);
                Results = new ArraySegment<int>(mergedAndSortedArray, 0, curMergedArrayIndex);
            }
        }

        private class GatherAllCollectorByReader : Collector
        {
            private ReaderFacetInfo _current;
            public readonly List<ReaderFacetInfo> Results = new List<ReaderFacetInfo>();

            public override void SetScorer(Scorer scorer)
            {
            }

            public override void Collect(int doc, IState state)
            {
                _current.AddMatch(doc);
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                _current = new ReaderFacetInfo
                {
                    DocBase = docBase,
                    Reader = reader
                };
                Results.Add(_current);
            }

            public override bool AcceptsDocsOutOfOrder => true;
        }
    }
}

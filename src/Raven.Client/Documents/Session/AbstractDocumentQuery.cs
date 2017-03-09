//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IAbstractDocumentQuery<T>
                                                            where TSelf : AbstractDocumentQuery<T, TSelf>
    {
        protected bool IsSpatialQuery;
        protected string SpatialFieldName, QueryShape;
        protected SpatialUnits? SpatialUnits;
        protected SpatialRelation SpatialRelation;
        protected double DistanceErrorPct;
        private readonly LinqPathProvider _linqPathProvider;
        protected Action<IndexQuery> BeforeQueryExecutionAction;

        protected readonly HashSet<Type> RootTypes = new HashSet<Type>
        {
            typeof (T)
        };

        private static Dictionary<Type, Func<object, string>> _implicitStringsCache = new Dictionary<Type, Func<object, string>>();

        /// <summary>
        /// Whatever to negate the next operation
        /// </summary>
        protected bool Negate;

        /// <summary>
        /// The index to query
        /// </summary>
        public string IndexName { get; }

        protected Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> TransformResultsFunc;

        protected string DefaultField;

        private int _currentClauseDepth;

        protected KeyValuePair<string, string> LastEquality;

        protected Dictionary<string, object> TransformerParameters = new Dictionary<string, object>();

        /// <summary>
        ///   The list of fields to project directly from the results
        /// </summary>
        protected internal readonly string[] ProjectionFields;

        /// <summary>
        ///   The list of fields to project directly from the index on the server
        /// </summary>
        protected readonly string[] FieldsToFetch;

        protected bool IsMapReduce;
        /// <summary>
        /// The session for this query
        /// </summary>
        protected readonly InMemoryDocumentSessionOperations TheSession;

        /// <summary>
        ///   The fields to order the results by
        /// </summary>
        protected string[] OrderByFields = new string[0];

        /// <summary>
        /// The fields of dynamic map-reduce query
        /// </summary>
        protected DynamicMapReduceField[] DynamicMapReduceFields = new DynamicMapReduceField[0];

        /// <summary>
        ///   The fields to highlight
        /// </summary>
        protected List<HighlightedField> HighlightedFields = new List<HighlightedField>();

        /// <summary>
        ///   Highlighter pre tags
        /// </summary>
        protected string[] HighlighterPreTags = new string[0];

        /// <summary>
        ///   Highlighter post tags
        /// </summary>
        protected string[] HighlighterPostTags = new string[0];

        /// <summary>
        ///   Highlighter key
        /// </summary>
        protected string HighlighterKeyName;

        /// <summary>
        ///   The page size to use when querying the index
        /// </summary>
        protected int? PageSize;

        protected QueryOperation QueryOperation;

        /// <summary>
        /// The query to use
        /// </summary>
        protected StringBuilder QueryText = new StringBuilder();

        /// <summary>
        ///   which record to start reading from
        /// </summary>
        protected int Start;

        private readonly DocumentConventions _conventions;
        /// <summary>
        /// Timeout for this query
        /// </summary>
        protected TimeSpan? Timeout;
        /// <summary>
        /// Should we wait for non stale results
        /// </summary>
        protected bool TheWaitForNonStaleResults;
        /// <summary>
        /// Should we wait for non stale results as of now?
        /// </summary>
        protected bool TheWaitForNonStaleResultsAsOfNow;
        /// <summary>
        /// The paths to include when loading the query
        /// </summary>
        protected HashSet<string> Includes = new HashSet<string>();

        /// <summary>
        /// Holds the query stats
        /// </summary>
        protected QueryStatistics QueryStats = new QueryStatistics();

        /// <summary>
        /// Holds the query highlightings
        /// </summary>
        protected QueryHighlightings Highlightings = new QueryHighlightings();

        /// <summary>
        /// The name of the results transformer to use after executing this query
        /// </summary>
        protected string Transformer;

        /// <summary>
        /// Determines if entities should be tracked and kept in memory
        /// </summary>
        protected bool DisableEntitiesTracking;

        /// <summary>
        /// Determine if query results should be cached.
        /// </summary>
        protected bool DisableCaching;

        /// <summary>
        /// Indicates if detailed timings should be calculated for various query parts (Lucene search, loading documents, transforming results). Default: false
        /// </summary>
        protected bool ShowQueryTimings;

        /// <summary>
        /// Determine if scores of query results should be explained
        /// </summary>
        protected bool ShouldExplainScores;

        public bool IsDistinct => _isDistinct;

        /// <summary>
        /// Gets the document convention from the query session
        /// </summary>
        public DocumentConventions Conventions => _conventions;

        /// <summary>
        ///   Gets the session associated with this document query
        /// </summary>
        public IDocumentSession Session => (IDocumentSession)TheSession;
        public IAsyncDocumentSession AsyncSession => (IAsyncDocumentSession)TheSession;

        public bool IsDynamicMapReduce => DynamicMapReduceFields.Length > 0;

        protected Action<QueryResult> AfterQueryExecutedCallback;
        protected AfterStreamExecutedDelegate AfterStreamExecutedCallback;
        protected long? CutoffEtag;

        private TimeSpan DefaultTimeout
        {
            get
            {
                if (Debugger.IsAttached) // increase timeout if we are debugging
                    return TimeSpan.FromMinutes(15);

                return TimeSpan.FromSeconds(15);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentQuery{T, TSelf}"/> class.
        /// </summary>
        protected AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
                                     string indexName,
                                     string[] fieldsToFetch,
                                     string[] projectionFields,
                                     bool isMapReduce)
        {
            ProjectionFields = projectionFields;
            FieldsToFetch = fieldsToFetch;
            IsMapReduce = isMapReduce;
            IndexName = indexName;
            TheSession = theSession;
            AfterQueryExecuted(UpdateStatsAndHighlightings);

            _conventions = theSession == null ? new DocumentConventions() : theSession.Conventions;
            _linqPathProvider = new LinqPathProvider(_conventions);
        }

        private void UpdateStatsAndHighlightings(QueryResult queryResult)
        {
            QueryStats.UpdateQueryStats(queryResult);
            Highlightings.Update(queryResult);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref = "IDocumentQuery{T}" /> class.
        /// </summary>
        /// <param name = "other">The other.</param>
        protected AbstractDocumentQuery(AbstractDocumentQuery<T, TSelf> other)
        {
            IndexName = other.IndexName;
            _linqPathProvider = other._linqPathProvider;
            AllowMultipleIndexEntriesForSameDocumentToResultTransformer =
                other.AllowMultipleIndexEntriesForSameDocumentToResultTransformer;
            ProjectionFields = other.ProjectionFields;
            TheSession = other.TheSession;
            _conventions = other._conventions;
            OrderByFields = other.OrderByFields;
            DynamicMapReduceFields = other.DynamicMapReduceFields;
            PageSize = other.PageSize;
            QueryText = other.QueryText;
            Start = other.Start;
            Timeout = other.Timeout;
            TheWaitForNonStaleResults = other.TheWaitForNonStaleResults;
            TheWaitForNonStaleResultsAsOfNow = other.TheWaitForNonStaleResultsAsOfNow;
            Includes = other.Includes;
            QueryStats = other.QueryStats;
            DefaultOperator = other.DefaultOperator;
            DefaultField = other.DefaultField;
            HighlightedFields = other.HighlightedFields;
            HighlighterPreTags = other.HighlighterPreTags;
            HighlighterPostTags = other.HighlighterPostTags;
            TransformerParameters = other.TransformerParameters;
            DisableEntitiesTracking = other.DisableEntitiesTracking;
            DisableCaching = other.DisableCaching;
            ShowQueryTimings = other.ShowQueryTimings;
            ShouldExplainScores = other.ShouldExplainScores;

            AfterQueryExecuted(UpdateStatsAndHighlightings);
        }

        #region TSelf Members

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance()
        {
            OrderBy(Constants.Documents.Indexing.Fields.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng, string sortedFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), sortedFieldName));
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        /// <param name="distErrorPercent">Gets the error distance that specifies how precise the query shape is.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude, double longitude, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(Constants.Documents.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        /// <param name = "radiusUnits">The units of the <paramref name="radius"/></param>
        /// <param name="distErrorPercent">Gets the error distance that specifies how precise the query shape is</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(Constants.Documents.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
            return this;
        }

        public IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distErrorPercent = 0.025)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }


        IDocumentQueryCustomization IDocumentQueryCustomization.RelatesToShape(string fieldName, string shapeWkt, SpatialRelation rel, double distErrorPercent)
        {
            GenerateSpatialQueryData(fieldName, shapeWkt, rel, distErrorPercent);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(new SpatialCriteriaFactory());
            GenerateSpatialQueryData(fieldName, criteria);
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        protected TSelf GenerateQueryWithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distanceErrorPct = 0.025, SpatialUnits? radiusUnits = null)
        {
            return GenerateSpatialQueryData(fieldName, SpatialIndexQuery.GetQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.Within, distanceErrorPct, radiusUnits);
        }

        protected TSelf GenerateSpatialQueryData(string fieldName, string shapeWkt, SpatialRelation relation, double distanceErrorPct = 0.025, SpatialUnits? radiusUnits = null)
        {
            IsSpatialQuery = true;
            SpatialFieldName = fieldName;
            QueryShape = new WktSanitizer().Sanitize(shapeWkt);
            SpatialRelation = relation;
            DistanceErrorPct = distanceErrorPct;
            SpatialUnits = radiusUnits;
            return (TSelf)this;
        }

        protected TSelf GenerateSpatialQueryData(string fieldName, SpatialCriteria criteria)
        {
            throw new NotImplementedException();
            var wkt = criteria.Shape as string;
            if (wkt == null && criteria.Shape != null)
            {
                var jsonSerializer = Conventions.CreateSerializer();

                /*using (var jsonWriter = new RavenJTokenWriter())
                {
                    var converter = new ShapeConverter();
                    jsonSerializer.Serialize(jsonWriter, criteria.Shape);
                    if (!converter.TryConvert(jsonWriter.Token, out wkt))
                        throw new ArgumentException("Shape");
                }*/
            }

            if (wkt == null)
                throw new ArgumentException("Shape");

            IsSpatialQuery = true;
            SpatialFieldName = fieldName;
            QueryShape = new WktSanitizer().Sanitize(wkt);
            SpatialRelation = criteria.Relation;
            DistanceErrorPct = criteria.DistanceErrorPct;
            return (TSelf)this;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        public void UsingDefaultField(string field)
        {
            DefaultField = field;
        }

        public void UsingDefaultOperator(QueryOperator @operator)
        {
            DefaultOperator = @operator;
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            var body = path.Body as UnaryExpression;
            if (body != null)
            {
                switch (body.NodeType)
                {
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        throw new InvalidOperationException("You cannot use Include<TResult> on value type. Please use the Include<TResult, TInclude> overload.");
                }
            }

            Include(path.ToPropertyPath());
            return this;
        }

        public IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path)
        {
            var idPrefix = Conventions.GetCollectionName(typeof(TInclude));
            if (idPrefix != null)
            {
                idPrefix = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(idPrefix);
                idPrefix += Conventions.IdentityPartsSeparator;
            }

            var id = path.ToPropertyPath() + "(" + idPrefix + ")";
            Include(id);
            return this;
        }

        /// <summary>
        ///   Instruct the query to wait for non stale result for the specified wait timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public void WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            TheWaitForNonStaleResults = true;
            CutoffEtag = null;
            Timeout = waitTimeout;
        }

        protected internal QueryOperation InitializeQueryOperation()
        {
            var indexQuery = GetIndexQuery();

            return new QueryOperation(TheSession,
                IndexName,
                indexQuery,
                ProjectionFields,
                TheWaitForNonStaleResults,
                Timeout,
                TransformResultsFunc,
                Includes,
                DisableEntitiesTracking);
        }

        public IndexQuery GetIndexQuery()
        {
            var query = QueryText.ToString();
            var indexQuery = GenerateIndexQuery(query);
            BeforeQueryExecutionAction?.Invoke(indexQuery);

            return indexQuery;
        }

        /// <summary>
        ///   Gets the fields for projection
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProjectionFields()
        {
            return ProjectionFields ?? Enumerable.Empty<string>();
        }

        /// <summary>
        /// Order the search results in alphanumeric order
        /// </summary>
        public void AlphaNumericOrdering(string fieldName, bool descending)
        {
            AddOrder(Constants.Documents.Indexing.Fields.AlphaNumericFieldName + ";" + fieldName, descending);
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        public void RandomOrdering()
        {
            AddOrder(Constants.Documents.Indexing.Fields.RandomFieldName + ";" + Guid.NewGuid(), false);
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        public void RandomOrdering(string seed)
        {
            AddOrder(Constants.Documents.Indexing.Fields.RandomFieldName + ";" + seed, false);
        }

        public void CustomSortUsing(string typeName, bool descending)
        {
            AddOrder(Constants.Documents.Indexing.Fields.CustomSortFieldName + ";" + typeName, descending);
        }

        public IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action)
        {
            BeforeQueryExecutionAction += action;
            return this;
        }

        public IDocumentQueryCustomization TransformResults(Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> resultsTransformer)
        {
            TransformResultsFunc = resultsTransformer;
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(val);
            return this;
        }

        public void AddMapReduceField(DynamicMapReduceField field)
        {
            IsMapReduce = true;

            DynamicMapReduceFields = DynamicMapReduceFields.Concat(new[] { field }).ToArray();
        }

        public DynamicMapReduceField[] GetGroupByFields()
        {
            return DynamicMapReduceFields.Where(x => x.IsGroupBy).ToArray();
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(preTag, postTag);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            SetHighlighterTags(preTags, postTags);
            return this;
        }

        public IDocumentQueryCustomization NoTracking()
        {
            DisableEntitiesTracking = true;
            return this;
        }

        public IDocumentQueryCustomization NoCaching()
        {
            DisableCaching = true;
            return this;
        }

        public IDocumentQueryCustomization ShowTimings()
        {
            ShowQueryTimings = true;
            return this;
        }

        public void SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(new[] { preTag }, new[] { postTag });
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public void AddOrder(string fieldName, bool descending)
        {
            fieldName = EnsureValidFieldName(new WhereParams
            {
                FieldName = fieldName
            });
            fieldName = descending ? "-" + fieldName : fieldName;
            OrderByFields = OrderByFields.Concat(new[] { fieldName }).ToArray();
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, fragmentsField));
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            fieldHighlightings = Highlightings.AddField(fieldName);
        }

        public void Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            HighlighterKeyName = fieldKeyName;
            HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            fieldHighlightings = Highlightings.AddField(fieldName);
        }

        public void SetHighlighterTags(string[] preTags, string[] postTags)
        {
            HighlighterPreTags = preTags;
            HighlighterPostTags = postTags;
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        public void Include(string path)
        {
            Includes.Add(path);
        }

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(
            @"
You cannot issue an in memory filter - such as Where(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Advanced.DocumentQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
"
            , true)]
        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }


        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(
            @"
You cannot issue an in memory filter - such as Count(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Advanced.DocumentQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
"
            , true)]
        public int Count(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        public void Include(Expression<Func<T, object>> path)
        {
            Include(path.ToPropertyPath());
        }

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Take(int count)
        {
            PageSize = count;
        }

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Skip(int count)
        {
            Start = count;
        }

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name = "whereClause">The where clause.</param>
        public void Where(string whereClause)
        {
            AppendSpaceIfNeeded(QueryText.Length > 0 && QueryText[QueryText.Length - 1] != '(');
            QueryText.Append(whereClause);
        }

        private void AppendSpaceIfNeeded(bool shouldAppendSpace)
        {
            if (shouldAppendSpace)
            {
                QueryText.Append(" ");
            }
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        public void WhereEquals(string fieldName, object value)
        {
            WhereEquals(new WhereParams
            {
                FieldName = fieldName,
                Value = value
            });
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to allow wildcards only if analyzed
        /// </remarks>
        public void WhereEquals(string fieldName, object value, bool isAnalyzed)
        {
            WhereEquals(new WhereParams
            {
                AllowWildcards = isAnalyzed,
                IsAnalyzed = isAnalyzed,
                FieldName = fieldName,
                Value = value
            });
        }


        /// <summary>
        ///   Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        public void OpenSubclause()
        {
            _currentClauseDepth++;
            AppendSpaceIfNeeded(QueryText.Length > 0 && QueryText[QueryText.Length - 1] != '(');
            NegateIfNeeded();
            QueryText.Append("(");
        }

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public void CloseSubclause()
        {
            _currentClauseDepth--;
            QueryText.Append(")");
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        public void WhereEquals(WhereParams whereParams)
        {
            EnsureValidFieldName(whereParams);

            var transformToEqualValue = TransformToEqualValue(whereParams);
            LastEquality = new KeyValuePair<string, string>(whereParams.FieldName, transformToEqualValue);

            AppendSpaceIfNeeded(QueryText.Length > 0 && QueryText[QueryText.Length - 1] != '(');
            NegateIfNeeded();

            QueryText.Append(RavenQuery.EscapeField(whereParams.FieldName));
            QueryText.Append(":");
            QueryText.Append(transformToEqualValue);
        }

        private string EnsureValidFieldName(WhereParams whereParams)
        {
            if (TheSession?.Conventions == null || whereParams.IsNestedPath)
                return whereParams.FieldName;

            if (IsMapReduce)
            {
                if (IsDynamicMapReduce)
                {
                    string name;
                    var rangeType = FieldUtil.GetRangeTypeFromFieldName(whereParams.FieldName, out name);

                    var renamedField = DynamicMapReduceFields.FirstOrDefault(x => x.ClientSideName == name);

                    if (renamedField != null)
                        return whereParams.FieldName = FieldUtil.ApplyRangeSuffixIfNecessary(renamedField.Name, rangeType);
                }

                return whereParams.FieldName;
            }

            foreach (var rootType in RootTypes)
            {
                var identityProperty = TheSession.Conventions.GetIdentityProperty(rootType);
                if (identityProperty != null && identityProperty.Name == whereParams.FieldName)
                {
                    return whereParams.FieldName = Constants.Documents.Indexing.Fields.DocumentIdFieldName;
                }
            }

            return whereParams.FieldName;
        }

        ///<summary>
        /// Negate the next operation
        ///</summary>
        public void NegateNext()
        {
            Negate = !Negate;
        }

        private void NegateIfNeeded()
        {
            if (Negate == false)
                return;
            Negate = false;
            QueryText.Append("-");
        }

        private IEnumerable<object> UnpackEnumerable(IEnumerable items)
        {
            foreach (var item in items)
            {
                var enumerable = item as IEnumerable;
                if (enumerable != null && item is string == false)
                {
                    foreach (var nested in UnpackEnumerable(enumerable))
                    {
                        yield return nested;
                    }
                }
                else
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        public void WhereIn(string fieldName, IEnumerable<object> values)
        {
            AppendSpaceIfNeeded(QueryText.Length > 0 && char.IsWhiteSpace(QueryText[QueryText.Length - 1]) == false);
            NegateIfNeeded();

            var whereParams = new WhereParams
            {
                FieldName = fieldName
            };
            fieldName = EnsureValidFieldName(whereParams);

            var list = UnpackEnumerable(values).ToList();

            if (list.Count == 0)
            {
                QueryText.Append("@emptyIn<")
                    .Append(RavenQuery.EscapeField(fieldName))
                    .Append(">:(no-results)");
                return;
            }

            QueryText.Append("@in<")
                .Append(RavenQuery.EscapeField(fieldName))
                .Append(">:(");

            AddItemToInClause(whereParams, list, first: true);
            QueryText.Append(") ");
        }

        private void AddItemToInClause(WhereParams whereParams, IEnumerable<object> list, bool first)
        {
            foreach (var value in list)
            {
                var enumerable = value as IEnumerable;
                if (enumerable != null && value is string == false)
                {
                    AddItemToInClause(whereParams, enumerable.Cast<object>(), first);
                    return;
                }
                if (first == false)
                {
                    QueryText.Append(" , ");
                }
                first = false;
                var nestedWhereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = whereParams.FieldName,
                    Value = value
                };
                QueryText.Append(TransformToEqualValue(nestedWhereParams).Replace(",", "`,`"));
            }
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereStartsWith(string fieldName, object value)
        {
            // NOTE: doesn't fully match StartsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat(value, "*"),
                    IsAnalyzed = true,
                    AllowWildcards = true
                });
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereEndsWith(string fieldName, object value)
        {
            // https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Wildcard%20Searches
            // You cannot use a * or ? symbol as the first character of a search

            // NOTE: doesn't fully match EndsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat("*", value),
                    AllowWildcards = true,
                    IsAnalyzed = true
                });
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetween(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(QueryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            QueryText.Append(RavenQuery.EscapeField(fieldName)).Append(":{");
            QueryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            QueryText.Append(" TO ");
            QueryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            QueryText.Append("}");
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(QueryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            QueryText.Append(RavenQuery.EscapeField(fieldName)).Append(":[");
            QueryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            QueryText.Append(" TO ");
            QueryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            QueryText.Append("]");
        }

        private string GetFieldNameForRangeQueries(string fieldName, object start, object end)
        {
            fieldName = EnsureValidFieldName(new WhereParams { FieldName = fieldName });

            if (fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return fieldName;

            var val = start ?? end;
            return FieldUtil.ApplyRangeSuffixIfNecessary(fieldName, val);
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThan(string fieldName, object value)
        {
            WhereBetween(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThan(string fieldName, object value)
        {
            WhereBetween(fieldName, null, value);
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, null, value);
        }

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        public void AndAlso()
        {
            if (QueryText.Length < 1)
                return;

            QueryText.Append(" AND");
        }

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        public void OrElse()
        {
            if (QueryText.Length < 1)
                return;

            QueryText.Append(" OR");
        }

        /// <summary>
        ///   Specifies a boost weight to the last where clause.
        ///   The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        public void Boost(decimal boost)
        {
            if (QueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (boost <= 0m)
            {
                throw new ArgumentOutOfRangeException(nameof(boost), "Boost factor must be a positive number");
            }

            if (boost != 1m)
            {
                // 1.0 is the default
                QueryText.Append("^").Append(boost.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        ///   Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        public void Fuzzy(decimal fuzzy)
        {
            if (QueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (fuzzy < 0m || fuzzy > 1m)
            {
                throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
            }

            var ch = QueryText[QueryText.Length - 1];
            if (ch == '"' || ch == ']')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
            }

            QueryText.Append("~");
            if (fuzzy != 0.5m)
            {
                // 0.5 is the default
                QueryText.Append(fuzzy.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        ///   Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name = "proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        public void Proximity(int proximity)
        {
            if (QueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (proximity < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(proximity), "Proximity distance must be a positive number");
            }

            if (QueryText[QueryText.Length - 1] != '"')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Proximity distance can only modify a phrase");
            }

            QueryText.Append("~").Append(proximity.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderBy(params string[] fields)
        {
            OrderByFields = OrderByFields.Concat(fields).ToArray();
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderByDescending(params string[] fields)
        {
            fields = fields.Select(MakeFieldSortDescending).ToArray();
            OrderBy(fields);
        }

        protected string MakeFieldSortDescending(string field)
        {
            if (string.IsNullOrWhiteSpace(field) || field.StartsWith("+") || field.StartsWith("-"))
            {
                return field;
            }

            return "-" + field;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow()
        {
            TheWaitForNonStaleResults = true;
            TheWaitForNonStaleResultsAsOfNow = true;
            Timeout = DefaultTimeout;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            TheWaitForNonStaleResults = true;
            TheWaitForNonStaleResultsAsOfNow = true;
            Timeout = waitTimeout;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, DefaultTimeout);
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
        {
            TheWaitForNonStaleResults = true;
            Timeout = waitTimeout;
            CutoffEtag = cutOffEtag;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        public void WaitForNonStaleResults()
        {
            WaitForNonStaleResults(DefaultTimeout);
        }

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        public void Statistics(out QueryStatistics stats)
        {
            stats = QueryStats;
        }

        /// <summary>
        /// Callback to get the results of the query
        /// </summary>
        public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
        {
            AfterQueryExecutedCallback += afterQueryExecutedCallback;
        }

        /// <summary>
        /// Callback to get the results of the stream
        /// </summary>
        public void AfterStreamExecuted(AfterStreamExecutedDelegate afterStreamExecutedCallback)
        {
            AfterStreamExecutedCallback += afterStreamExecutedCallback;
        }

        /// <summary>
        /// Called externally to raise the after query executed callback
        /// </summary>
        public void InvokeAfterQueryExecuted(QueryResult result)
        {
            AfterQueryExecutedCallback?.Invoke(result);
        }

        /// <summary>
        /// Called externally to raise the after stream executed callback
        /// </summary>
        public void InvokeAfterStreamExecuted(BlittableJsonReaderObject result)
        {
            AfterStreamExecutedCallback?.Invoke(result);
        }

        #endregion

        /// <summary>
        ///   Generates the index query.
        /// </summary>
        /// <param name = "query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            if (IsSpatialQuery)
            {
                if (IndexName == "dynamic" || IndexName.StartsWith("dynamic/"))
                    throw new NotSupportedException("Dynamic indexes do not support spatial queries. A static index, with spatial field(s), must be defined.");

                var spatialQuery = new SpatialIndexQuery(_conventions)
                {
                    IsDistinct = _isDistinct,
                    Query = query,
                    Start = Start,
                    WaitForNonStaleResultsAsOfNow = TheWaitForNonStaleResultsAsOfNow,
                    WaitForNonStaleResults = TheWaitForNonStaleResults,
                    WaitForNonStaleResultsTimeout = Timeout,
                    CutoffEtag = CutoffEtag,
                    SortedFields = OrderByFields.Select(x => new SortedField(x)).ToArray(),
                    DynamicMapReduceFields = DynamicMapReduceFields,
                    FieldsToFetch = FieldsToFetch,
                    SpatialFieldName = SpatialFieldName,
                    QueryShape = QueryShape,
                    RadiusUnitOverride = SpatialUnits,
                    SpatialRelation = SpatialRelation,
                    DistanceErrorPercentage = DistanceErrorPct,
                    DefaultField = DefaultField,
                    DefaultOperator = DefaultOperator,
                    HighlightedFields = HighlightedFields.Select(x => x.Clone()).ToArray(),
                    HighlighterPreTags = HighlighterPreTags.ToArray(),
                    HighlighterPostTags = HighlighterPostTags.ToArray(),
                    HighlighterKeyName = HighlighterKeyName,
                    Transformer = Transformer,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    TransformerParameters = TransformerParameters,
                    DisableCaching = DisableCaching,
                    ShowTimings = ShowQueryTimings,
                    ExplainScores = ShouldExplainScores,
                    Includes = Includes.ToArray()
                };

                if (PageSize.HasValue)
                    spatialQuery.PageSize = PageSize.Value;

                return spatialQuery;
            }

            var indexQuery = new IndexQuery(_conventions)
            {
                IsDistinct = _isDistinct,
                Query = query,
                Start = Start,
                CutoffEtag = CutoffEtag,
                WaitForNonStaleResultsAsOfNow = TheWaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResults = TheWaitForNonStaleResults,
                WaitForNonStaleResultsTimeout = Timeout,
                SortedFields = OrderByFields.Select(x => new SortedField(x)).ToArray(),
                DynamicMapReduceFields = DynamicMapReduceFields,
                FieldsToFetch = FieldsToFetch,
                DefaultField = DefaultField,
                DefaultOperator = DefaultOperator,
                HighlightedFields = HighlightedFields.Select(x => x.Clone()).ToArray(),
                HighlighterPreTags = HighlighterPreTags.ToArray(),
                HighlighterPostTags = HighlighterPostTags.ToArray(),
                HighlighterKeyName = HighlighterKeyName,
                Transformer = Transformer,
                TransformerParameters = TransformerParameters,
                AllowMultipleIndexEntriesForSameDocumentToResultTransformer = AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                DisableCaching = DisableCaching,
                ShowTimings = ShowQueryTimings,
                ExplainScores = ShouldExplainScores,
                Includes = Includes.ToArray()
            };

            if (PageSize != null)
                indexQuery.PageSize = PageSize.Value;

            return indexQuery;
        }

        private static readonly Regex EscapePostfixWildcard = new Regex(@"\\\*(\s|$)", RegexOptions.Compiled);
        protected QueryOperator DefaultOperator;
        protected bool _isDistinct;
        protected bool AllowMultipleIndexEntriesForSameDocumentToResultTransformer;

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public void Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
        {
            QueryText.Append(' ');

            NegateIfNeeded();
            switch (escapeQueryOptions)
            {
                case EscapeQueryOptions.EscapeAll:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    break;
                case EscapeQueryOptions.AllowPostfixWildcard:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = EscapePostfixWildcard.Replace(searchTerms, "*${1}");
                    break;
                case EscapeQueryOptions.AllowAllWildcards:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = searchTerms.Replace("\\*", "*");
                    break;
                case EscapeQueryOptions.RawQuery:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(escapeQueryOptions), "Value: " + escapeQueryOptions);
            }
            bool hasWhiteSpace = searchTerms.Any(char.IsWhiteSpace);
            LastEquality = new KeyValuePair<string, string>(fieldName,
                hasWhiteSpace ? "(" + searchTerms + ")" : searchTerms
                );

            QueryText.Append(fieldName).Append(":").Append("(").Append(searchTerms).Append(")");
        }

        private string TransformToEqualValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
            {
                return Constants.Documents.Indexing.Fields.NullValueNotAnalyzed;
            }
            if (Equals(whereParams.Value, string.Empty))
            {
                return Constants.Documents.Indexing.Fields.EmptyStringNotAnalyzed;
            }

            var type = whereParams.Value.GetType().GetNonNullableType();

            if (_conventions.SaveEnumsAsIntegers && type.GetTypeInfo().IsEnum)
            {
                return ((int)whereParams.Value).ToString();
            }

            if (type == typeof(bool))
            {
                return (bool)whereParams.Value ? "true" : "false";
            }
            if (type == typeof(DateTime))
            {
                var val = (DateTime)whereParams.Value;
                var s = val.GetDefaultRavenFormat();
                if (val.Kind == DateTimeKind.Utc)
                    s += "Z";
                return s;
            }
            if (type == typeof(DateTimeOffset))
            {
                var val = (DateTimeOffset)whereParams.Value;
                return val.UtcDateTime.GetDefaultRavenFormat(true);
            }

            if (type == typeof(decimal))
            {
                return RavenQuery.Escape(((double)((decimal)whereParams.Value)).ToString(CultureInfo.InvariantCulture), false, false);
            }

            if (type == typeof(double))
            {
                return RavenQuery.Escape(((double)(whereParams.Value)).ToString("r", CultureInfo.InvariantCulture), false, false);
            }
            var strValue = whereParams.Value as string;
            if (strValue != null)
            {
                strValue = RavenQuery.Escape(strValue,
                        whereParams.AllowWildcards && whereParams.IsAnalyzed, whereParams.IsAnalyzed);

                return whereParams.IsAnalyzed ? strValue : String.Concat("[[", strValue, "]]");
            }

            if (_conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Equality, out strValue))
                return strValue;

            if (whereParams.Value is ValueType)
            {
                var escaped = RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                                                whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

                return escaped;
            }

            var result = GetImplicitStringConvertion(whereParams.Value.GetType());
            if (result != null)
            {
                return RavenQuery.Escape(result(whereParams.Value), whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            }

            throw new NotImplementedException();
            /*
            var jsonSerializer = _conventions.CreateSerializer();
            var ravenJTokenWriter = new RavenJTokenWriter();
            jsonSerializer.Serialize(ravenJTokenWriter, whereParams.Value);
            var term = ravenJTokenWriter.Token.ToString(Formatting.None);
            if (term.Length > 1 && term[0] == '"' && term[term.Length - 1] == '"')
            {
                term = term.Substring(1, term.Length - 2);
            }
            switch (ravenJTokenWriter.Token.Type)
            {
                case JTokenType.Object:
                case JTokenType.Array:
                    return "[[" + RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, false) + "]]";

                default:
                    return RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            }
            */
        }

        private Func<object, string> GetImplicitStringConvertion(Type type)
        {
            if (type == null)
                return null;

            Func<object, string> value;
            var localStringsCache = _implicitStringsCache;
            if (localStringsCache.TryGetValue(type, out value))
                return value;

            var methodInfo = type.GetMethod("op_Implicit", new[] { type });

            if (methodInfo == null || methodInfo.ReturnType != typeof(string))
            {
                _implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
                {
                    {type, null}
                };
                return null;
            }

            var arg = Expression.Parameter(typeof(object), "self");

            var func = (Func<object, string>)Expression.Lambda(Expression.Call(methodInfo, Expression.Convert(arg, type)), arg).Compile();

            _implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
                {
                    {type, func}
                };
            return func;
        }

        private string TransformToRangeValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
                return Constants.Documents.Indexing.Fields.NullValueNotAnalyzed;
            if (Equals(whereParams.Value, string.Empty))
                return Constants.Documents.Indexing.Fields.EmptyStringNotAnalyzed;

            if (whereParams.Value is DateTime)
            {
                var dateTime = (DateTime)whereParams.Value;
                var dateStr = dateTime.GetDefaultRavenFormat();
                if (dateTime.Kind == DateTimeKind.Utc)
                    dateStr += "Z";
                return dateStr;
            }
            if (whereParams.Value is DateTimeOffset)
                return ((DateTimeOffset)whereParams.Value).UtcDateTime.GetDefaultRavenFormat(true);

            if (whereParams.Value is int)
                return NumberUtil.NumberToString((int)whereParams.Value);
            if (whereParams.Value is long)
                return NumberUtil.NumberToString((long)whereParams.Value);
            if (whereParams.Value is decimal)
                return NumberUtil.NumberToString((double)(decimal)whereParams.Value);
            if (whereParams.Value is double)
                return NumberUtil.NumberToString((double)whereParams.Value);
            if (whereParams.Value is TimeSpan)
                return NumberUtil.NumberToString(((TimeSpan)whereParams.Value).Ticks);
            if (whereParams.Value is float)
                return NumberUtil.NumberToString((float)whereParams.Value);
            if (whereParams.Value is string)
                return RavenQuery.Escape(whereParams.Value.ToString(), false, true);

            string strVal;
            if (_conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Range,
                                                    out strVal))
                return strVal;

            if (whereParams.Value is ValueType)
                return RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                                         false, true);

            var stringWriter = new StringWriter();
            _conventions.CreateSerializer().Serialize(stringWriter, whereParams.Value);

            var sb = stringWriter.GetStringBuilder();
            if (sb.Length > 1 && sb[0] == '"' && sb[sb.Length - 1] == '"')
            {
                sb.Remove(sb.Length - 1, 1);
                sb.Remove(0, 1);
            }

            return RavenQuery.Escape(sb.ToString(), false, true);
        }

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents the query for this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents the query for this instance.
        /// </returns>
        public override string ToString()
        {
            if (_currentClauseDepth != 0)
                throw new InvalidOperationException(string.Format("A clause was not closed correctly within this query, current clause depth = {0}", _currentClauseDepth));

            return QueryText.ToString().Trim();
        }

        /// <summary>
        /// The last term that we asked the query to use equals on
        /// </summary>
        public KeyValuePair<string, string> GetLastEqualityTerm(bool isAsync = false)
        {
            return LastEquality;
        }

        public void Intersect()
        {
            QueryText.Append(Constants.Documents.Querying.IntersectSeparator);
        }

        public void ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "OR");
        }

        public void ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "AND");
        }

        private void ContainsAnyAllProcessor(string fieldName, IEnumerable<object> values, string seperator)
        {
            AppendSpaceIfNeeded(QueryText.Length > 0 && char.IsWhiteSpace(QueryText[QueryText.Length - 1]) == false);
            NegateIfNeeded();

            var list = UnpackEnumerable(values).ToList();
            if (list.Count == 0)
            {
                QueryText.Append("*:*");
                return;
            }

            var first = true;
            QueryText.Append("(");
            foreach (var value in list)
            {
                if (first == false)
                {
                    QueryText.Append(" " + seperator + " ");
                }
                first = false;
                var whereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = fieldName,
                    Value = value
                };
                EnsureValidFieldName(whereParams);
                QueryText.Append(fieldName)
                         .Append(":")
                         .Append(TransformToEqualValue(whereParams));
            }
            QueryText.Append(")");
        }

        public void AddRootType(Type type)
        {
            RootTypes.Add(type);
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder(string fieldName, bool descending)
        {
            AddOrder(fieldName, descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            AddOrder(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AlphaNumericOrdering(string fieldName, bool descending)
        {
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AlphaNumericOrdering<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            AlphaNumericOrdering(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName)
        {
            CustomSortUsing(typeName, false);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }

        public string GetMemberQueryPathForOrderBy(Expression expression)
        {
            var memberQueryPath = GetMemberQueryPath(expression);
            var memberExpression = _linqPathProvider.GetMemberExpression(expression);
            return FieldUtil.ApplyRangeSuffixIfNecessary(memberQueryPath, memberExpression.Type);
        }

        public string GetMemberQueryPath(Expression expression)
        {
            var result = _linqPathProvider.GetPath(expression);
            result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);

            if (expression.NodeType == ExpressionType.ArrayLength)
                result.Path += ".Length";

            var propertyName = IndexName == null || IndexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
                ? _conventions.FindPropertyNameForDynamicIndex(typeof(T), IndexName, "", result.Path)
                : _conventions.FindPropertyNameForIndex(typeof(T), IndexName, "", result.Path);
            return propertyName;
        }

        public void SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(
            bool val)
        {
            AllowMultipleIndexEntriesForSameDocumentToResultTransformer =
                val;
        }

        public void SetTransformer(string transformer)
        {
            Transformer = transformer;
        }

        public void Distinct()
        {
            _isDistinct = true;
        }
    }
}

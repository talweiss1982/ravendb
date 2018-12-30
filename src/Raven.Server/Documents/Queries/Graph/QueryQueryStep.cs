﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryQueryStep : IGraphQueryStep
    {
        public readonly Query Query;
        private StringSegment _alias;
        private HashSet<string> _aliases;
        private DocumentsOperationContext _context;
        private long? _resultEtag;
        private OperationCancelToken _token;
        private QueryRunner _queryRunner;
        private QueryMetadata _queryMetadata;
        private readonly Sparrow.Json.BlittableJsonReaderObject _queryParameters;
        private List<Match> _temp = new List<Match>();


        private int _index = -1;
        private GraphQueryResults _results = new GraphQueryResults();
        private Dictionary<string, Match> _resultsById = new Dictionary<string, Match>(StringComparer.OrdinalIgnoreCase);
        private GraphQueryPlan _graphQueryPlan;

        public QueryQueryStep(QueryRunner queryRunner, StringSegment alias, Query query, QueryMetadata queryMetadata, Sparrow.Json.BlittableJsonReaderObject queryParameters, DocumentsOperationContext documentsContext, long? existingResultEtag,
            GraphQueryPlan gqp, OperationCancelToken token)
        {
            _graphQueryPlan = gqp;
            Query = query;
            _alias = alias;
            _aliases = new HashSet<string> { _alias.Value };
            _queryRunner = queryRunner;
            _queryMetadata = queryMetadata;
            _queryParameters = queryParameters;
            _context = documentsContext;
            _resultEtag = existingResultEtag;
            _token = token;

            if (!string.IsNullOrEmpty(queryMetadata.CollectionName)) //not a '_' collection
            {
                try
                {
                    var _ = _queryRunner.Database.DocumentsStorage.GetCollection(queryMetadata.CollectionName, throwIfDoesNotExist: true);
                }
                catch (Exception e)
                {
                    throw new InvalidQueryException("Query on collection " + queryMetadata.CollectionName + " failed, because there is no such collection. If you meant to use " + queryMetadata.CollectionName + " as an alias, use: (_ as " + queryMetadata.CollectionName +")", e );
                }
            }
        }

        public bool CanBeConsideredForDestinationOptimization
        {
            get
            {
                if (HasWhereClause) //TODO: verify with Tal how good is this change
                    return false;

                if (_queryMetadata.IsCollectionQuery == false)
                    return false;

                if (_queryMetadata.HasIncludeOrLoad || _queryMetadata.Query.Limit != null || _queryMetadata.Query.Offset != null )
                    return false;

                return true;
            }
        }

        public bool HasWhereClause => Query.Where != null;

        public static CollectionDestinationQueryStep ToCollectionDestinationQueryStep(DocumentsStorage documentsStorage, QueryQueryStep qqs)
        {
            return new CollectionDestinationQueryStep(qqs._alias, qqs._context, documentsStorage, qqs._queryMetadata.CollectionName);
        }

        public bool IsEmpty()
        {
            return _results.Count == 0;
        }

        public bool CollectIntermediateResults { get; set; }

        public List<Match> IntermediateResults => CollectIntermediateResults ? _results.Results : new List<Match>();

        public IGraphQueryStep Clone()
        {
            return new QueryQueryStep(_queryRunner, _alias, Query, _queryMetadata, _queryParameters, _context, _resultEtag, _graphQueryPlan, _token)
            {
                CollectIntermediateResults = CollectIntermediateResults
            };
        }

        public bool GetNext(out Match match)
        {
            if (_index >= _results.Count)
            {
                match = default;
                return false;
            }
            match = _results[_index++];
            return true;
        }

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            var results = _queryRunner.ExecuteQuery(new IndexQueryServerSide(_queryMetadata)
            {
                QueryParameters = _queryParameters,
            },
                  _context, _resultEtag, _token);

            if (results.IsCompleted)
            {
                // most of the time, we complete in a sync fashion
                CompleteInitialization(results.Result);
                return default;
            }
            
            return new ValueTask(CompleteInitializeAsync(results));
        }

        private async Task CompleteInitializeAsync(Task<DocumentQueryResult> results)
        {
            CompleteInitialization(await results);
        }

        private void CompleteInitialization(DocumentQueryResult results)
        {
            _graphQueryPlan.IsStale |= results.IsStale;
            _index = 0;
            foreach (var result in results.Results)
            {
                var match = new Match();
                match.Set(_alias, result);
                _results.Add(match);
                if (result.Id == null)
                    continue;
                _resultsById[result.Id] = match;
            }
        }

        public List<Match> GetById(string id)
        {
            if(_results.Count != 0 && _resultsById.Count == 0)// only reason is that we are projecting non documents here
                throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");
              
            _temp.Clear();
            if (_resultsById.TryGetValue(id, out var match))
                _temp.Add(match);
            return _temp;

        }

        public string GetOutputAlias()
        {
            return _alias.Value;
        }

        public string GetIndexName => _queryMetadata.IndexName;

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        public void Analyze(Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            var result = match.GetResult(_alias.Value);
            if (result == null)
                return;

            if(result is Document d && d.Id != null)
            {
                graphDebugInfo.AddNode(d.Id.ToString(), d);
            }
            else
            {
                graphDebugInfo.AddNode(null, result);
            }
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return new QuerySingleStep(this);
    }

        private class QuerySingleStep : ISingleGraphStep
        {
            private QueryQueryStep _parent;
            private List<Match> _temp = new List<Match>(1);

            public QuerySingleStep(QueryQueryStep queryQueryStep)
            {
                _parent = queryQueryStep;
}


            public void AddAliases(HashSet<string> aliases)
            {
                aliases.UnionWith(_parent.GetAllAliases());
            }

            public void SetPrev(IGraphQueryStep prev)
            {
            }

            public bool GetAndClearResults(List<Match> matches)
            {
                if (_temp.Count == 0)
                    return false;

                matches.AddRange(_temp);

                _temp.Clear();

                return true;
            }

            public ValueTask Initialize()
            {
                return _parent.Initialize();
            }

            public void Run(Match src, string alias)
            {
                // here we already get the right match, and we do nothing with it.
                var clone = new Match(src);
                clone.Remove(alias);
                clone.Set(_parent.GetOutputAlias(), src.GetResult(alias));
                _temp.Add(clone);
            }
        }

        public void CreateIdenticalQueryWithDiffrentAlias(StringSegment alias)
        {
            var clone = Clone() as QueryQueryStep;
            clone._alias = alias;
            clone._aliases = new HashSet<string>{_alias.ToString()};
            clone._results = _results;
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner : AbstractQueryRunner
    {
        private const int NumberOfRetries = 3;

        private readonly GraphQueryRunner _graph;
        private readonly StaticIndexQueryRunner _static;
        private readonly AbstractQueryRunner _dynamic;
        private readonly CollectionQueryRunner _collection;
        private int _nextQueryId;

        public QueryRunner(DocumentDatabase database) : base(database)
        {
            _static = new StaticIndexQueryRunner(database);
            _dynamic = database.Configuration.Indexing.DisableQueryOptimizerGeneratedIndexes
                ? (AbstractQueryRunner)new InvalidQueryRunner(database)
                : new DynamicQueryRunner(database);
            _collection = new CollectionQueryRunner(database);
            _graph = new GraphQueryRunner(database);
            CurrentlyRunningQueries = new ConcurrentSet<ExecutingQueryInfo>();
        }

        internal long GetNextQueryId() => Interlocked.Increment(ref _nextQueryId);
        internal readonly ConcurrentSet<ExecutingQueryInfo> CurrentlyRunningQueries;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AbstractQueryRunner GetRunner(IndexQueryServerSide query)
        {
            if (query.Metadata.IsDynamic)
            {
                if (query.Metadata.IsCollectionQuery == false)
                    return _dynamic;

                return _collection;
            }
            if (query.Metadata.IsGraph)
                return _graph;

            return _static;
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    Stopwatch sw = null;
                    QueryTimingsScope scope;
                    DocumentQueryResult result;
                    using (scope = query.Timings?.Start())
                    {
                        if (scope == null)
                            sw = Stopwatch.StartNew();

                        result = await GetRunner(query).ExecuteQuery(query, documentsContext, existingResultEtag, token);
                    }

                    result.DurationInMs = sw != null ? (long)sw.Elapsed.TotalMilliseconds : (long)scope.Duration.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    await GetRunner(query).ExecuteStreamQuery(query, documentsContext, response, writer, token);
                    return;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    documentsContext.CloseTransaction();
                    await GetRunner(query).ExecuteStreamIndexEntriesQuery(query, documentsContext, response, writer, token);
                    return;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }
        public async Task<FacetedQueryResult> ExecuteFacetedQuery(IndexQueryServerSide query, long? existingResultEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    var sw = Stopwatch.StartNew();

                    var result = await _static.ExecuteFacetedQuery(query, existingResultEtag, documentsContext, token);

                    result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public TermsQueryResultServerSide ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token, out Index index)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {

                try
                {
                    index = GetIndex(indexName);

                    var etag = index.GetIndexEtag(null);
                    if (etag == existingResultEtag)
                        return TermsQueryResultServerSide.NotModifiedResult;

                    return index.GetTerms(field, fromValue, pageSize, context, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    Stopwatch sw = null;
                    QueryTimingsScope scope;
                    SuggestionQueryResult result;
                    using (scope = query.Timings?.Start())
                    {
                        if (scope == null)
                            sw = Stopwatch.StartNew();

                        result = await GetRunner(query).ExecuteSuggestionQuery(query, context, existingResultEtag, token);
                    }

                    result.DurationInMs = sw != null ? (long)sw.Elapsed.TotalMilliseconds : (long)scope.Duration.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecuteIndexEntriesQuery(query, context, existingResultEtag, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecuteDeleteQuery(query, options, context, onProgress, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            ObjectDisposedException lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecutePatchQuery(query, options, patch, patchArgs, context, onProgress, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(IndexQueryServerSide query, DocumentsOperationContext context, out string indexName)
        {
            if (query.Metadata.IsDynamic == false)
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            if (_dynamic is DynamicQueryRunner d)
                return d.ExplainIndexSelection(query, context, out indexName);

            throw new NotSupportedException(InvalidQueryRunner.ErrorMessage);
        }

        private static Exception CreateRetriesFailedException(Exception inner)
        {
            return new InvalidOperationException($"Could not execute query. Tried {NumberOfRetries} times.", inner);
        }
    }
}

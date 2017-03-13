//-----------------------------------------------------------------------
// <copyright file="IDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents
{

    /// <summary>
    /// Interface for managing access to RavenDB and open sessions.
    /// </summary>
    public interface IDocumentStore : IDisposalNotification
    {
        /// <summary>
        /// Store events
        /// </summary>
        event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        event EventHandler<AfterStoreEventArgs> OnAfterStore;

        /// <summary>
        /// Delete event
        /// </summary>
        event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;

        /// <summary>
        /// Query event
        /// </summary>
        event EventHandler<BeforeQueryExecutedEventArgs> OnBeforeQueryExecuted;

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        IDatabaseChanges Changes(string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCacheFor(TimeSpan cacheDuration);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCache();

        /// <summary>
        /// Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        /// This is mainly useful for internal use inside RavenDB, when we are executing
        /// queries that has been marked with WaitForNonStaleResults, we temporarily disable
        /// aggressive caching.
        /// </remarks>
        IDisposable DisableAggressiveCaching();

        /// <summary>
        /// Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        /// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        IDisposable SetRequestsTimeoutFor(TimeSpan timeout);

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        string Identifier { get; set; }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        IDocumentStore Initialize();

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentSession OpenAsyncSession();

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentSession OpenAsyncSession(string database);

        /// <summary>
        /// Opens the async session with the specified options.
        /// </summary>
        IAsyncDocumentSession OpenAsyncSession(SessionOptions sessionOptions);

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        IDocumentSession OpenSession();

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
        IDocumentSession OpenSession(string database);

        /// <summary>
        /// Opens the session with the specified options.
        /// </summary>
        IDocumentSession OpenSession(SessionOptions sessionOptions);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        void ExecuteIndex(AbstractIndexCreationTask task);

        void ExecuteIndexes(IEnumerable<AbstractIndexCreationTask> tasks);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        /// <param name="task"></param>
        Task ExecuteIndexAsync(AbstractIndexCreationTask task, CancellationToken token = default(CancellationToken));

        Task ExecuteIndexesAsync(IEnumerable<AbstractIndexCreationTask> tasks, CancellationToken token = default(CancellationToken));

        /// <summary>
        /// Executes the transformer creation
        /// </summary>
        void ExecuteTransformer(AbstractTransformerCreationTask task);

        Task ExecuteTransformerAsync(AbstractTransformerCreationTask task, CancellationToken token = default(CancellationToken));

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        DocumentConventions Conventions { get; }

        /// <summary>
        /// Gets the URL.
        /// </summary>
        string Url { get; }

        BulkInsertOperation BulkInsert(string database = null);

        /// <summary>
        /// Provides methods to manage data subscriptions in async manner.
        /// </summary>
        IAsyncReliableSubscriptions AsyncSubscriptions { get; }

        /// <summary>
        /// Provides methods to manage data subscriptions.
        /// </summary>
        IReliableSubscriptions Subscriptions { get; }

        string DefaultDatabase { get; set; }

        RequestExecutor GetRequestExecuter(string databaseName = null);

        AdminOperationExecuter Admin { get; }

        OperationExecuter Operations { get; }
    }
}

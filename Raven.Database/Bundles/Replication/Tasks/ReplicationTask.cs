//-----------------------------------------------------------------------
// <copyright file="ReplicationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Bundles.Replication.Tasks.Handlers;
using Raven.Database.Config.Retriever;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Database.Bundles.Replication;

namespace Raven.Bundles.Replication.Tasks
{
    using Database.Indexing;

    [ExportMetadata("Bundle", "Replication")]
    [InheritedExport(typeof(IStartupTask))]
    public class ReplicationTask : IStartupTask, IDisposable
    {
        public bool IsRunning { get; private set; }

        private volatile bool shouldPause;

        public const int SystemDocsLimitForRemoteEtagUpdate = 15;
        public const int DestinationDocsLimitForRemoteEtagUpdate = 15;

        public readonly ConcurrentQueue<Task> activeTasks = new ConcurrentQueue<Task>();

        private readonly ConcurrentDictionary<string, DestinationStats> destinationStats =
            new ConcurrentDictionary<string, DestinationStats>(StringComparer.OrdinalIgnoreCase);

        private DocumentDatabase docDb;
        private readonly static ILog log = LogManager.GetCurrentClassLogger();
        private bool firstTimeFoundNoReplicationDocument = true;
        private bool wrongReplicationSourceAlertSent;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> activeReplicationTasks = new ConcurrentDictionary<string, SemaphoreSlim>();

        private readonly TaskCompletionSource<object> tcsReplicationOnce = new TaskCompletionSource<object>();

        private readonly ConcurrentDictionary<string, DateTime> destinationAlertSent = new ConcurrentDictionary<string, DateTime>();

        private readonly ConcurrentDictionary<string, bool> destinationForceBuffering = new ConcurrentDictionary<string, bool>();

        public ConcurrentDictionary<string, DestinationStats> DestinationStats
        {
            get { return destinationStats; }
        }

        public event Action ReplicationExecuted;

        public ConcurrentDictionary<string, DateTime> Heartbeats
        {
            get { return heartbeatDictionary; }
        }

        private int replicationAttempts;
        private int workCounter;
        private HttpRavenRequestFactory httpRavenRequestFactory;

        private IndependentBatchSizeAutoTuner autoTuner;
        private readonly ConcurrentDictionary<string, PrefetchingBehavior> prefetchingBehaviors = new ConcurrentDictionary<string, PrefetchingBehavior>();

        public IndexReplicationTask IndexReplication { get; set; }

        public TransformerReplicationTask TransformerReplication { get; set; }

        public void Execute(DocumentDatabase database)
        {
            docDb = database;

            var replicationRequestTimeoutInMs = docDb.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

            autoTuner = new IndependentBatchSizeAutoTuner(docDb.WorkContext, PrefetchingUser.Replicator);
            httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = replicationRequestTimeoutInMs };

            var task = new Task(Execute, TaskCreationOptions.LongRunning);
            var disposableAction = new DisposableAction(task.Wait);
            // make sure that the doc db waits for the replication task shutdown
            docDb.ExtensionsState.GetOrAdd(Guid.NewGuid().ToString(), s => disposableAction);

            DeployIndexesAndTransformers(database);

            IndexReplication = new IndexReplicationTask(database, httpRavenRequestFactory, this);
            TransformerReplication = new TransformerReplicationTask(database, httpRavenRequestFactory, this);

            task.Start();

            IndexReplication.Start();
            TransformerReplication.Start();
        }

        private static void DeployIndexesAndTransformers(DocumentDatabase database)
        {
            try
            {
                var index = new RavenConflictDocuments();
                database.Indexes.PutIndex(index.IndexName, index.CreateIndexDefinition());
            }
            catch (Exception e)
            {
                log.ErrorException("Could not deploy 'Raven/ConflictDocuments' index.", e);
            }

            try
            {
                var transformer = new RavenConflictDocumentsTransformer();
                database.Transformers.PutTransform(transformer.TransformerName, transformer.CreateTransformerDefinition());
            }
            catch (Exception e)
            {
                log.ErrorException("Could not deploy 'Raven/ConflictDocumentsTransformer' transformer.", e);
            }
        }

        public void Pause()
        {
            shouldPause = true;
            Console.WriteLine($"Pausing replication task at {docDb.TransactionalStorage.Id}");
        }

        public void Continue()
        {
            shouldPause = false;
            Console.WriteLine($"Continue replication task at {docDb.TransactionalStorage.Id}");
        }

        private bool IsHotSpare()
        {
            if (docDb.RequestManager == null) return false;
            return docDb.RequestManager.IsInHotSpareMode;
        }

        private void Execute()
        {
            using (LogContext.WithDatabase(docDb.Name))
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replication task started.");

                var name = GetType().Name;

                var timeToWait = TimeSpan.FromMinutes(5);
                bool runningBecauseOfDataModifications = false;
                var context = docDb.WorkContext;
                NotifySiblings();
                while (context.DoWork)
                {
                    IsRunning = !IsHotSpare() && !shouldPause;
                    if (log.IsDebugEnabled)
                        log.Debug("Replication task found work. Running: {0}", IsRunning);

                    if (IsRunning)
                    {
                        try
                        {
                            ExecuteReplicationOnce(runningBecauseOfDataModifications);
                        }
                        catch (Exception e)
                        {
                            log.ErrorException("Failed to perform replication", e);
                        }
                    }

                    runningBecauseOfDataModifications = context.WaitForWork(timeToWait, ref workCounter, name);

                    timeToWait = runningBecauseOfDataModifications
                        ?TimeSpan.FromMilliseconds(0)
                        : TimeSpan.FromSeconds(15);
                    /* ? TimeSpan.FromSeconds(30)
                     : TimeSpan.FromMinutes(5);*/
                }

                IsRunning = false;
            }
        }

        public Task ExecuteReplicationOnce(bool runningBecauseOfDataModifications)
        {
            using (docDb.DisableAllTriggersForCurrentThread())
            {
                var destinations = GetReplicationDestinations();

                if (destinations.Length == 0)
                {
                    WarnIfNoReplicationTargetsWereFound();
                    tcsReplicationOnce.SetResult(null);
                    return tcsReplicationOnce.Task;
                }
                
                var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

                var copyOfrunningBecauseOfDataModifications = runningBecauseOfDataModifications;
                var destinationForReplication = destinations.Where(
                    dest =>
                    {
                        if (copyOfrunningBecauseOfDataModifications == false) return true;
                        return IsNotFailing(dest, currentReplicationAttempts);
                    }).ToList();

                CleanupPrefetchingBehaviors(destinations.Select(x => x.ConnectionStringOptions.Url),
                    destinations.Except(destinationForReplication).Select(x => x.ConnectionStringOptions.Url));

                var startedTasks = new List<Task>();

                foreach (var dest in destinationForReplication)
                {
                    var destination = dest;
                    var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, s => new SemaphoreSlim(1));
                    if (holder.Wait(0) == false)
                    {
                        if (log.IsDebugEnabled)
                            log.Debug("Replication to distination {0} skipped due to existing replication operation", dest.ConnectionStringOptions.Url);
                        continue;
                    }

                    var replicationTask = Task.Factory.StartNew(
                        () =>
                        {
                            using (LogContext.WithDatabase(docDb.Name))
                            using (CultureHelper.EnsureInvariantCulture())
                            {
                                try
                                {
                                    if (ReplicateTo(destination))
                                        docDb.WorkContext.NotifyAboutWork();
                                }
                                catch (Exception e)
                                {
                                    log.ErrorException("Could not replicate to " + destination, e);
                                }
                            }
                        });

                    startedTasks.Add(replicationTask);

                    activeTasks.Enqueue(replicationTask);
                    replicationTask.ContinueWith(
                        _ =>
                        {
                            // here we purge all the completed tasks at the head of the queue
                            Task task;
                            while (activeTasks.TryPeek(out task))
                            {
                                if (!task.IsCompleted && !task.IsCanceled && !task.IsFaulted) break;
                                activeTasks.TryDequeue(out task); // remove it from end
                            }
                        });
                }

                return Task.WhenAll(startedTasks.ToArray()).ContinueWith(
                    t =>
                    {
                        if (destinationStats.Count == 0)
                            return;

                        foreach (var stats in destinationStats.Where(stats => stats.Value.LastReplicatedEtag != null))
                        {
                            PrefetchingBehavior prefetchingBehavior;
                            if (prefetchingBehaviors.TryGetValue(stats.Key, out prefetchingBehavior))
                            {
                                prefetchingBehavior.CleanupDocuments(stats.Value.LastReplicatedEtag);
                            }
                        }
                    }).ContinueWith(t => OnReplicationExecuted()).AssertNotFailed();
            }
        }

        private void CleanupPrefetchingBehaviors(IEnumerable<string> allDestinations, IEnumerable<string> failingDestinations)
        {
            PrefetchingBehavior prefetchingBehaviorToDispose;

            // remove prefetching behaviors for non-existing destinations
            foreach (var removedDestination in prefetchingBehaviors.Keys.Except(allDestinations))
            {
                if (prefetchingBehaviors.TryRemove(removedDestination, out prefetchingBehaviorToDispose))
                {
                    prefetchingBehaviorToDispose.Dispose();
                }
            }

            // also remove prefetchers if the destination is failing for a long time
            foreach (var failingDestination in failingDestinations)
            {
                DestinationStats stats;
                if (prefetchingBehaviors.ContainsKey(failingDestination) == false || destinationStats.TryGetValue(failingDestination, out stats) == false)
                    continue;

                if (stats.FirstFailureInCycleTimestamp != null && stats.LastFailureTimestamp != null &&
                    stats.LastFailureTimestamp - stats.FirstFailureInCycleTimestamp >= TimeSpan.FromMinutes(3))
                {
                    if (prefetchingBehaviors.TryRemove(failingDestination, out prefetchingBehaviorToDispose))
                    {
                        prefetchingBehaviorToDispose.Dispose();
                    }
                }
            }
        }

        private void NotifySiblings()
        {
            var notifications = new BlockingCollection<RavenConnectionStringOptions>();

            Task.Factory.StartNew(() => NotifySibling(notifications));

            int skip = 0;
            var replicationDestinations = GetReplicationDestinations();
            foreach (var replicationDestination in replicationDestinations)
            {
                notifications.TryAdd(replicationDestination.ConnectionStringOptions, 15 * 1000);
            }

            while (true)
            {
                int nextPageStart = skip; // will trigger rapid pagination
                var docs = docDb.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, skip, 128, CancellationToken.None, ref nextPageStart);
                if (docs.Length == 0)
                {
                    notifications.TryAdd(null, 15 * 1000); // marker to stop notify this
                    return;
                }

                skip += docs.Length;

                foreach (RavenJObject doc in docs)
                {
                    var sourceReplicationInformation = doc.JsonDeserialization<SourceReplicationInformation>();
                    if (string.IsNullOrEmpty(sourceReplicationInformation.Source))
                        continue;

                    var match = replicationDestinations.FirstOrDefault(x =>
                                                           string.Equals(x.ConnectionStringOptions.Url,
                                                                         sourceReplicationInformation.Source,
                                                                         StringComparison.OrdinalIgnoreCase));

                    if (match != null)
                    {
                        notifications.TryAdd(match.ConnectionStringOptions, 15 * 1000);
                    }
                    else
                    {
                        notifications.TryAdd(new RavenConnectionStringOptions
                        {
                            Url = sourceReplicationInformation.Source
                        }, 15 * 1000);
                    }
                }
            }
        }

        private void NotifySibling(BlockingCollection<RavenConnectionStringOptions> collection)
        {
            using (LogContext.WithDatabase(docDb.Name))
                while (true)
                {
                    RavenConnectionStringOptions connectionStringOptions;
                    try
                    {
                        collection.TryTake(out connectionStringOptions, 15 * 1000, docDb.WorkContext.CancellationToken);
                        if (connectionStringOptions == null)
                            return;
                    }
                    catch (Exception e)
                    {
                        log.ErrorException("Could not get connection string options to notify sibling servers about restart", e);
                        return;
                    }
                    try
                    {
                        var url = connectionStringOptions.Url + "/replication/heartbeat?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;
                        var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
                        request.WebRequest.ContentLength = 0;
                        request.ExecuteRequest();
                    }
                    catch (Exception e)
                    {
                        log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
                    }
                }
        }

        private bool IsNotFailing(ReplicationStrategy dest, int currentReplicationAttempts)
        {
            var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(dest.ConnectionStringOptions.Url), null);
            if (jsonDocument == null)
                return true;
            var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
            if (failureInformation.FailureCount > 1000)
            {
                var shouldReplicateTo = currentReplicationAttempts % 10 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 100)
            {
                var shouldReplicateTo = currentReplicationAttempts % 5 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 10)
            {
                var shouldReplicateTo = currentReplicationAttempts % 2 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            return true;
        }

        public static string EscapeDestinationName(string url)
        {
            return Uri.EscapeDataString(url.Replace("https://", "").Replace("http://", "").Replace("/", "").Replace(":", ""));
        }

        private void WarnIfNoReplicationTargetsWereFound()
        {
            if (firstTimeFoundNoReplicationDocument)
            {
                firstTimeFoundNoReplicationDocument = false;
                log.Warn("Replication bundle is installed, but there is no destination in 'Raven/Replication/Destinations'.\r\nReplication results in NO-OP");
            }
        }

        private bool ReplicateTo(ReplicationStrategy destination)
        {
            try
            {
                if (docDb.Disposed)
                    return false;

                using (docDb.DisableAllTriggersForCurrentThread())
                using (var stats = new ReplicationStatisticsRecorder(destination, destinationStats))
                {
                    SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource;
                    using (var scope = stats.StartRecording("Destination"))
                    {
                        try
                        {
                            destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);
                            if (destinationsReplicationInformationForSource == null)
                            {
                                destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);

                                if (destinationsReplicationInformationForSource == null)
                                {
                                    log.Error("Failed to replicate documents to destination {0}, because was not able to receive last Etag", destination.ConnectionStringOptions.Url);
                                    return false;
                                }
                            }
                                    
                            if (destinationsReplicationInformationForSource.LastDocumentEtag == Etag.Empty &&
                                destinationsReplicationInformationForSource.LastAttachmentEtag == Etag.Empty)
                            {
                                IndexReplication.Execute();
                            }

                            scope.Record(RavenJObject.FromObject(destinationsReplicationInformationForSource));

                            if (destinationsReplicationInformationForSource.LastDocumentEtag == Etag.InvalidEtag &&
                                destinationsReplicationInformationForSource.LastAttachmentEtag == Etag.InvalidEtag &&
                                (destination.SpecifiedCollections == null || destination.SpecifiedCollections.Count == 0))
                            {
                                DateTime lastSent;

                                // todo: move lastModifiedDate after the condition
                                var lastModifiedDate = destinationsReplicationInformationForSource.LastModified.HasValue ? destinationsReplicationInformationForSource.LastModified.Value.ToLocalTime() : DateTime.MinValue;

                                if (destinationAlertSent.TryGetValue(destination.ConnectionStringOptions.Url, out lastSent) && (SystemTime.UtcNow - lastSent).TotalMinutes < 1)
                                {
                                    if (log.IsDebugEnabled) // todo: remove this log line
                                        log.Debug(string.Format(@"Destination server is forbidding replication due to a possibility of having multiple instances with same DatabaseId replicating to it. After 10 minutes from '{2}' another instance will start replicating. Destination Url: {0}. DatabaseId: {1}. Current source: {3}. Stored source on destination: {4}.", destination.ConnectionStringOptions.Url, docDb.TransactionalStorage.Id, lastModifiedDate, docDb.ServerUrl, destinationsReplicationInformationForSource.Source));
                                    return false;
                                }

                                docDb.AddAlert(new Alert
                                {
                                    AlertLevel = AlertLevel.Error,
                                    CreatedAt = SystemTime.UtcNow,
                                    Message = string.Format(@"Destination server is forbidding replication due to a possibility of having multiple instances with same DatabaseId replicating to it. After 10 minutes from '{2}' another instance will start replicating. Destination Url: {0}. DatabaseId: {1}. Current source: {3}. Stored source on destination: {4}.", destination.ConnectionStringOptions.Url, docDb.TransactionalStorage.Id, lastModifiedDate, docDb.ServerUrl, destinationsReplicationInformationForSource.Source),
                                    Title = string.Format("Replication error. Multiple databases replicating at the same time with same DatabaseId ('{0}') detected.", docDb.TransactionalStorage.Id),
                                    UniqueKey = "Replication to " + destination.ConnectionStringOptions.Url + " errored. Wrong DatabaseId: " + docDb.TransactionalStorage.Id
                                });

                                destinationAlertSent.AddOrUpdate(destination.ConnectionStringOptions.Url, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);

                                return false;
                            }
                        }
                        catch (Exception e)
                        {
                            scope.RecordError(e);
                            log.WarnException("Failed to replicate to: " + destination, e);
                            return false;
                        }
                    }

                    bool? replicated = null;

                    int replicatedDocuments;

                    using (var scope = stats.StartRecording("Documents"))
                    {
                        switch (ReplicateDocuments(destination, destinationsReplicationInformationForSource, scope, out replicatedDocuments))
                        {
                            case true:
                                replicated = true;
                                break;
                            case false:
                                return false;
                        }
                    }

                    using (var scope = stats.StartRecording("Attachments"))
                    {
                        switch (ReplicateAttachments(destination, destinationsReplicationInformationForSource, scope))
                        {
                            case true:
                                replicated = true;
                                break;
                            case false:
                                return false;
                        }
                    }

                    var elapsedMicroseconds = (long)(stats.ElapsedTime.Ticks * SystemTime.MicroSecPerTick);
                    docDb.WorkContext.MetricsCounters.GetReplicationDurationHistogram(destination).Update(elapsedMicroseconds);
                    UpdateReplicationPerformance(destination, stats.Started, stats.ElapsedTime, replicatedDocuments);

                    return replicated ?? false;
                }
            }
            finally
            {
                var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, s => new SemaphoreSlim(0, 1));
                holder.Release();
            }
        }

        private void UpdateReplicationPerformance(ReplicationStrategy destination, DateTime startTime, TimeSpan elapsed, int batchSize)
        {
            if (batchSize > 0)
            {
                var queue = docDb.WorkContext.MetricsCounters.GetReplicationPerformanceStats(destination);
                queue.Enqueue(new ReplicationPerformanceStats
                {
                    Duration = elapsed,
                    Started = startTime,
                    BatchSize = batchSize
                });

                while (queue.Count() > 25)
                {
                    ReplicationPerformanceStats _;
                    queue.TryDequeue(out _);
                }
            }
        }


        [Obsolete("Use RavenFS instead.")]
        private bool? ReplicateAttachments(ReplicationStrategy destination, SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder)
        {
            Tuple<RavenJArray, Etag> tuple;
            RavenJArray attachments;

            using (var scope = recorder.StartRecording("Get"))
            {
                tuple = GetAttachments(destinationsReplicationInformationForSource, destination, scope);
                attachments = tuple.Item1;

                if (attachments == null || attachments.Length == 0)
                {
                    if (tuple.Item2 != destinationsReplicationInformationForSource.LastAttachmentEtag)
                    {
                        SetLastReplicatedEtagForServer(destination, lastAttachmentEtag: tuple.Item2);
                    }
                    return null;
                }
            }

            using (var scope = recorder.StartRecording("Send"))
            {
                string lastError;
                if (TryReplicationAttachments(destination, attachments, out lastError) == false) // failed to replicate, start error handling strategy
                {
                    if (IsFirstFailure(destination.ConnectionStringOptions.Url))
                    {
                        log.Info("This is the first failure for {0}, assuming transient failure and trying again", destination);
                        if (TryReplicationAttachments(destination, attachments, out lastError)) // success on second fail
                        {
                            RecordSuccess(destination.ConnectionStringOptions.Url, lastReplicatedEtag: tuple.Item2);
                            return true;
                        }
                    }

                    scope.RecordError(lastError);
                    RecordFailure(destination.ConnectionStringOptions.Url, lastError);
                    return false;
                }
            }

            RecordSuccess(destination.ConnectionStringOptions.Url,
                lastReplicatedEtag: tuple.Item2);

            return true;
        }

        private bool? ReplicateDocuments(ReplicationStrategy destination, SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder, out int replicatedDocuments)
        {
            replicatedDocuments = 0;
            JsonDocumentsToReplicate documentsToReplicate = null;
            var sp = Stopwatch.StartNew();
            IDisposable removeBatch = null;

            var prefetchingBehavior = prefetchingBehaviors.GetOrAdd(destination.ConnectionStringOptions.Url,
                x => docDb.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Replicator, autoTuner, string.Format("Replication for URL: {0}", destination.ConnectionStringOptions.DefaultDatabase)));

            prefetchingBehavior.AdditionalInfo = string.Format("For destination: {0}. Last replicated etag: {1}", destination.ConnectionStringOptions.Url, destinationsReplicationInformationForSource.LastDocumentEtag);

            try
            {
                using (var scope = recorder.StartRecording("Get"))
                {
                    documentsToReplicate = GetJsonDocuments(destinationsReplicationInformationForSource, destination, prefetchingBehavior, scope);
                    if (documentsToReplicate.Documents == null || documentsToReplicate.Documents.Length == 0)
                    {
                        if (documentsToReplicate.LastEtag != destinationsReplicationInformationForSource.LastDocumentEtag)
                        {
                            // we don't notify remote server about updates to system docs, see: RavenDB-715
                            if (documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments == 0
                                || documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments > SystemDocsLimitForRemoteEtagUpdate
                                || documentsToReplicate.CountOfFilteredDocumentsWhichOriginFromDestination > DestinationDocsLimitForRemoteEtagUpdate) // see RavenDB-1555
                            {
                                using (scope.StartRecording("Notify"))
                                {
                                    SetLastReplicatedEtagForServer(destination, lastDocEtag: documentsToReplicate.LastEtag);
                                    scope.Record(new RavenJObject
                                             {
                                                 { "LastDocEtag", documentsToReplicate.LastEtag.ToString() }
                                             });
                                }
                            }
                        }
                        RecordLastEtagChecked(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag);
                        return null;
                    }
                }

                // if the db is idling in all respect except sending out replication, let us keep it that way.
                docDb.WorkContext.UpdateFoundWork();

                removeBatch = prefetchingBehavior.UpdateCurrentlyUsedBatches(documentsToReplicate.LoadedDocs);

                using (var scope = recorder.StartRecording("Send"))
                {
                    string lastError;
                    if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError) == false) // failed to replicate, start error handling strategy
                    {
                        if (IsFirstFailure(destination.ConnectionStringOptions.Url))
                        {
                            log.Info(
                                "This is the first failure for {0}, assuming transient failure and trying again",
                                destination);
                            if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError)) // success on second fail
                            {
                                RecordSuccess(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
                                return true;
                            }
                        }
                        // if we had an error sending to this endpoint, it might be because we are sending too much data, or because
                        // the request timed out. This will let us know that the next time we try, we'll use just the initial doc counts
                        // and we'll be much more conservative with increasing the sizes
                        prefetchingBehavior.OutOfMemoryExceptionHappened();
                        scope.RecordError(lastError);
                        RecordFailure(destination.ConnectionStringOptions.Url, lastError);
                        return false;
                    }
                }
            }
            finally
            {
                if (documentsToReplicate != null && documentsToReplicate.LoadedDocs != null)
                {
                    prefetchingBehavior.UpdateAutoThrottler(documentsToReplicate.LoadedDocs, sp.Elapsed);
                    replicatedDocuments = documentsToReplicate.LoadedDocs.Count;
                }

                if (removeBatch != null)
                    removeBatch.Dispose();
            }

            RecordSuccess(destination.ConnectionStringOptions.Url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
            return true;
        }

        private void SetLastReplicatedEtagForServer(ReplicationStrategy destination, Etag lastDocEtag = null, Etag lastAttachmentEtag = null)
        {
            try
            {
                var url = GetUrlFor(destination, "/replication/lastEtag");

                if (lastDocEtag != null)
                    url += "&docEtag=" + lastDocEtag;
                if (lastAttachmentEtag != null)
                    url += "&attachmentEtag=" + lastAttachmentEtag;

                var request = httpRavenRequestFactory.Create(url, HttpMethods.Put, destination.ConnectionStringOptions, GetRequestBuffering(destination));
                request.Write(new byte[0]);
                request.ExecuteRequest();
                if (log.IsDebugEnabled)
                    log.Debug("Sent last replicated document Etag {0} to server {1}", lastDocEtag, destination.ConnectionStringOptions.Url);
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
                    log.WarnException("Replication is not enabled on: " + destination, e);
                else
                    log.WarnException("Failed to contact replication destination: " + destination, e);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to contact replication destination: " + destination, e);
            }
        }

        private string GetUrlFor(ReplicationStrategy destination, string endpoint)
        {
            var url = destination.ConnectionStringOptions.Url + endpoint + "?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;

            if (destination.SpecifiedCollections == null || destination.SpecifiedCollections.Count > 0)
                return url;

            return url + ("&collections=" + string.Join(";", destination.SpecifiedCollections.Keys));
        }

        private void RecordFailure(string url, string lastError)
        {
            var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            var failureCount = Interlocked.Increment(ref stats.FailureCountInternal);
            stats.LastFailureTimestamp = SystemTime.UtcNow;

            if (stats.FirstFailureInCycleTimestamp == null)
                stats.FirstFailureInCycleTimestamp = SystemTime.UtcNow;

            if (string.IsNullOrWhiteSpace(lastError) == false)
                stats.LastError = lastError;

            var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null);
            var failureInformation = new DestinationFailureInformation { Destination = url };
            if (jsonDocument != null)
            {
                failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
                // we only want to update this once a minute, otherwise we have churn with starting replication
                // because we are writing a failure document
                if ((SystemTime.UtcNow - jsonDocument.LastModified.GetValueOrDefault()).TotalMinutes < 1)
                {
                    return;
                }
            }
            failureInformation.FailureCount = failureCount;
            docDb.Documents.Put(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null,
                      RavenJObject.FromObject(failureInformation), new RavenJObject(), null);
        }

        private void RecordLastEtagChecked(string url, Etag lastEtagChecked)
        {
            var stats = destinationStats.GetOrDefault(url, new DestinationStats { Url = url });
            stats.LastEtagCheckedForReplication = lastEtagChecked;
        }

        private void RecordSuccess(string url,
            Etag lastReplicatedEtag = null, DateTime? lastReplicatedLastModified = null,
            DateTime? lastHeartbeatReceived = null, string lastError = null)
        {
            var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            Interlocked.Exchange(ref stats.FailureCountInternal, 0);
            stats.LastSuccessTimestamp = SystemTime.UtcNow;
            stats.FirstFailureInCycleTimestamp = null;

            if (lastReplicatedEtag != null)
            {
                stats.LastEtagCheckedForReplication = lastReplicatedEtag;
                stats.LastReplicatedEtag = lastReplicatedEtag;
            }

            if (lastReplicatedLastModified.HasValue)
                stats.LastReplicatedLastModified = lastReplicatedLastModified;

            if (lastHeartbeatReceived.HasValue)
                stats.LastHeartbeatReceived = lastHeartbeatReceived;

            if (!string.IsNullOrWhiteSpace(lastError))
                stats.LastError = lastError;

            docDb.Documents.Delete(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null, null);
        }

        private bool IsFirstFailure(string url)
        {
            var destStats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            return destStats.FailureCount == 0;
        }

        [Obsolete("Use RavenFS instead.")]
        private bool TryReplicationAttachments(ReplicationStrategy destination, RavenJArray jsonAttachments, out string errorMessage)
        {
            try
            {
                var url = destination.ConnectionStringOptions.Url + "/replication/replicateAttachments?from=" +
                          UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;

                var sp = Stopwatch.StartNew();
                using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
                {
                    var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, GetRequestBuffering(destination));

                    request.WriteBson(jsonAttachments);
                    request.ExecuteRequest(docDb.WorkContext.CancellationToken);
                    log.Info("Replicated {0} attachments to {1} in {2:#,#;;0} ms", jsonAttachments.Length, destination, sp.ElapsedMilliseconds);
                    errorMessage = "";
                    return true;
                }
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                if (response != null)
                {
                    using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
                    {
                        var error = streamReader.ReadToEnd();
                        try
                        {
                            var ravenJObject = RavenJObject.Parse(error);
                            log.WarnException("Replication to " + destination + " had failed\r\n" + ravenJObject.Value<string>("Error"), e);
                            errorMessage = error;
                            return false;
                        }
                        catch (Exception)
                        {
                        }

                        log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
                        errorMessage = error;
                    }
                }
                else
                {
                    log.WarnException("Replication to " + destination + " had failed", e);
                    errorMessage = e.Message;
                }
                return false;
            }
            catch (Exception e)
            {
                log.WarnException("Replication to " + destination + " had failed", e);
                errorMessage = e.Message;
                return false;
            }
        }

        internal bool GetRequestBuffering(ReplicationStrategy destination)
        {
            return destinationForceBuffering.GetOrAdd(destination.ConnectionStringOptions.Url, docDb.Configuration.Replication.ForceReplicationRequestBuffering);
        }

        private bool TryReplicationDocuments(ReplicationStrategy destination, RavenJArray jsonDocuments, out string lastError)
        {
            try
            {
                if (log.IsDebugEnabled)
                    log.Debug("Starting to replicate {0} documents to {1}", jsonDocuments.Length, destination);

                var url = GetUrlFor(destination, "/replication/replicateDocs");

                url += "&count=" + jsonDocuments.Length;

                var sp = Stopwatch.StartNew();

                using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
                {
                    var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, GetRequestBuffering(destination));
                    request.Write(jsonDocuments);
                    request.ExecuteRequest(docDb.WorkContext.CancellationToken);

                    log.Info("Replicated {0} documents to {1} in {2:#,#;;0} ms", jsonDocuments.Length, destination, sp.ElapsedMilliseconds);
                    lastError = "";
                    return true;
                }
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                if (response != null)
                {
                    var responseStream = response.GetResponseStream();
                    if (responseStream != null)
                    {
                        using (var streamReader = new StreamReader(responseStream))
                        {
                            var error = streamReader.ReadToEnd();
                            log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
                        }
                    }
                    else
                    {
                        log.WarnException("Replication to " + destination + " had failed", e);
                    }
                }
                else
                {
                    log.WarnException("Replication to " + destination + " had failed", e);
                }
                lastError = e.Message;
                return false;
            }
            catch (Exception e)
            {
                log.WarnException("Replication to " + destination + " had failed", e);
                lastError = e.Message;
                return false;
            }
        }

        internal void HandleRequestBufferingErrors(Exception e, ReplicationStrategy destination)
        {
            if (destination.ConnectionStringOptions.Credentials != null && string.Equals(e.Message, "This request requires buffering data to succeed.", StringComparison.OrdinalIgnoreCase))
                destinationForceBuffering.AddOrUpdate(destination.ConnectionStringOptions.Url, true, (s, b) => true);
        }

        private class JsonDocumentsToReplicate
        {
            public Etag LastEtag { get; set; }
            public DateTime LastLastModified { get; set; }
            public RavenJArray Documents { get; set; }
            public int CountOfFilteredDocumentsWhichAreSystemDocuments { get; set; }
            public int CountOfFilteredDocumentsWhichOriginFromDestination { get; set; }
            public List<JsonDocument> LoadedDocs { get; set; }
        }

        private JsonDocumentsToReplicate GetJsonDocuments(SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStrategy destination, PrefetchingBehavior prefetchingBehavior, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
        {
            var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
            var duration = Stopwatch.StartNew();
            var result = new JsonDocumentsToReplicate
            {
                LastEtag = Etag.Empty,
            };
            try
            {
                var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
                var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;

                docDb.TransactionalStorage.Batch(actions =>
                {
                    var lastEtag = destinationsReplicationInformationForSource.LastDocumentEtag;

                    int docsSinceLastReplEtag = 0;
                    List<JsonDocument> fetchedDocs;
                    List<JsonDocument> docsToReplicate;
                    result.LastEtag = lastEtag;

                    while (true)
                    {
                        docDb.WorkContext.CancellationToken.ThrowIfCancellationRequested();

                        fetchedDocs = GetDocsToReplicate(actions, prefetchingBehavior, result.LastEtag, maxNumberOfItemsToReceiveInSingleBatch);

                        IEnumerable<JsonDocument> handled = fetchedDocs;

                        foreach (var handler in new IReplicatedDocsHandler[]
                                {
                            new FilterReplicatedDocs(docDb.Documents, destination, prefetchingBehavior, destinationId, result.LastEtag),
                            new FilterAndTransformSpecifiedCollections(docDb, destination, destinationId)
                        })
                                    {
                            handled = handler.Handle(handled);
                                        }

                        docsToReplicate = handled.ToList();								

                        docsSinceLastReplEtag += fetchedDocs.Count;
                        result.CountOfFilteredDocumentsWhichAreSystemDocuments +=
                            fetchedDocs.Count(doc => destination.IsSystemDocumentId(doc.Key));
                        result.CountOfFilteredDocumentsWhichOriginFromDestination +=
                            fetchedDocs.Count(doc => destination.OriginsFromDestination(destinationId, doc.Metadata));

                        if (fetchedDocs.Count > 0)
                        {
                            var lastDoc = fetchedDocs.Last();
                            Debug.Assert(lastDoc.Etag != null);
                            result.LastEtag = lastDoc.Etag;
                            if (lastDoc.LastModified.HasValue)
                                result.LastLastModified = lastDoc.LastModified.Value;
                        }

                        if (fetchedDocs.Count == 0 || docsToReplicate.Count != 0)
                        {
                            break;
                        }

                        if (log.IsDebugEnabled)
                            log.Debug("All the docs were filtered, trying another batch from etag [>{0}]", result.LastEtag);

                        if (duration.Elapsed > timeout)
                            break;
                    }

                    if (log.IsDebugEnabled)
                        log.Debug(() =>
                        {
                            if (docsSinceLastReplEtag == 0)
                                return string.Format("No documents to replicate to {0} - last replicated etag: {1}", destination,
                                    lastEtag);

                            if (docsSinceLastReplEtag == docsToReplicate.Count)
                                return string.Format("Replicating {0} docs [>{1}] to {2}.",
                                    docsSinceLastReplEtag,
                                    lastEtag,
                                    destination);

                            var diff = fetchedDocs.Except(docsToReplicate).Select(x => x.Key);
                            return string.Format("Replicating {1} docs (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
                                docsSinceLastReplEtag,
                                docsToReplicate.Count,
                                destination,
                                string.Join(", ", diff),
                                lastEtag);
                        });

                    scope.Record(new RavenJObject
                    {
                        {"StartEtag", lastEtag.ToString()},
                        {"EndEtag", result.LastEtag.ToString()},
                        {"Count", docsSinceLastReplEtag},
                        {"FilteredCount", docsToReplicate.Count}
                    });

                    result.LoadedDocs = docsToReplicate;
                    docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeMetric(destination).Mark(docsSinceLastReplEtag);
                    docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeHistogram(destination).Update(docsSinceLastReplEtag);

                    result.Documents = new RavenJArray(docsToReplicate
                        .Select(x =>
                        {
                            JsonDocument.EnsureIdInMetadata(x);
                            EnsureReplicationInformationInMetadata(x.Metadata, docDb);
                            return x;
                        })
                        .Select(x => x.ToJson()));
                });
            }
            catch (Exception e)
            {
                scope.RecordError(e);
                log.WarnException(
                    "Could not get documents to replicate after: " +
                    destinationsReplicationInformationForSource.LastDocumentEtag, e);
            }
            return result;
        }

        private List<JsonDocument> GetDocsToReplicate(IStorageActionsAccessor actions, PrefetchingBehavior prefetchingBehavior, Etag from, int? maxNumberOfItemsToReceiveInSingleBatch)
        {
            var docsToReplicate = prefetchingBehavior.GetDocumentsBatchFrom(from, maxNumberOfItemsToReceiveInSingleBatch);
            Etag lastEtag = null;
            if (docsToReplicate.Count > 0)
                lastEtag = docsToReplicate[docsToReplicate.Count - 1].Etag;

            var maxNumberOfTombstones = Math.Max(1024, docsToReplicate.Count);
            var tombstones = actions
                .Lists
                .Read(Constants.RavenReplicationDocsTombstones, from, lastEtag, maxNumberOfTombstones + 1)
                .Select(x => new JsonDocument
                {
                    Etag = x.Etag,
                    Key = x.Key,
                    Metadata = x.Data,
                    DataAsJson = new RavenJObject()
                })
                .ToList();

            var results = docsToReplicate.Concat(tombstones);

            if (tombstones.Count >= maxNumberOfTombstones + 1)
            {
                var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
                log.Info("Replication batch trimmed. Found more than '{0}' document tombstones. Last etag from prefetcher: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

                results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
            }

            results = results.OrderBy(x => x.Etag);

            // can't return earlier, because we need to know if there are tombstones that need to be send
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

            return results.ToList();
        }

        [Obsolete("Use RavenFS instead.")]
        private Tuple<RavenJArray, Etag> GetAttachments(SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStrategy destination, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
        {
            var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
            var duration = Stopwatch.StartNew();

            RavenJArray attachments = null;
            Etag lastAttachmentEtag = Etag.Empty;
            try
            {
                var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
                var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;

                docDb.TransactionalStorage.Batch(actions =>
                {
                    int attachmentSinceLastEtag = 0;
                    List<AttachmentInformation> attachmentsToReplicate;
                    List<AttachmentInformation> filteredAttachmentsToReplicate;
                    var startEtag = destinationsReplicationInformationForSource.LastAttachmentEtag;
                    lastAttachmentEtag = startEtag;
                    while (true)
                    {
                        attachmentsToReplicate = GetAttachmentsToReplicate(actions, lastAttachmentEtag, maxNumberOfItemsToReceiveInSingleBatch);

                        filteredAttachmentsToReplicate = attachmentsToReplicate.Where(attachment => destination.FilterAttachments(attachment, destinationId)).ToList();

                        attachmentSinceLastEtag += attachmentsToReplicate.Count;

                        if (attachmentsToReplicate.Count == 0 ||
                            filteredAttachmentsToReplicate.Count != 0)
                        {
                            break;
                        }

                        AttachmentInformation jsonDocument = attachmentsToReplicate.Last();
                        Etag attachmentEtag = jsonDocument.Etag;
                        if (log.IsDebugEnabled)
                            log.Debug("All the attachments were filtered, trying another batch from etag [>{0}]", attachmentEtag);
                        lastAttachmentEtag = attachmentEtag;

                        if (duration.Elapsed > timeout)
                            break;
                    }

                    if (log.IsDebugEnabled)
                        log.Debug(() =>
                        {
                            if (attachmentSinceLastEtag == 0)
                                return string.Format("No attachments to replicate to {0} - last replicated etag: {1}", destination,
                                                     destinationsReplicationInformationForSource.LastAttachmentEtag);

                            if (attachmentSinceLastEtag == filteredAttachmentsToReplicate.Count)
                                return string.Format("Replicating {0} attachments [>{1}] to {2}.",
                                                 attachmentSinceLastEtag,
                                                 destinationsReplicationInformationForSource.LastAttachmentEtag,
                                                 destination);

                            var diff = attachmentsToReplicate.Except(filteredAttachmentsToReplicate).Select(x => x.Key);
                            return string.Format("Replicating {1} attachments (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
                                                 attachmentSinceLastEtag,
                                                 filteredAttachmentsToReplicate.Count,
                                                 destination,
                                                 string.Join(", ", diff),
                                                 destinationsReplicationInformationForSource.LastAttachmentEtag);
                        });

                    scope.Record(new RavenJObject
                                 {
                                     {"StartEtag", startEtag.ToString()},
                                     {"EndEtag", lastAttachmentEtag.ToString()},
                                     {"Count", attachmentSinceLastEtag},
                                     {"FilteredCount", filteredAttachmentsToReplicate.Count}
                                 });

                    attachments = new RavenJArray(filteredAttachmentsToReplicate
                                                      .Select(x =>
                                                      {
                                                          var data = new byte[0];
                                                          if (x.Size > 0)
                                                          {
                                                              data = actions.Attachments.GetAttachment(x.Key).Data().ReadData();
                                                          }

                                                          EnsureReplicationInformationInMetadata(x.Metadata, docDb);

                                                          return new RavenJObject
                                                                       {
                                                                           {"@metadata", x.Metadata},
                                                                           {"@id", x.Key},
                                                                           {"@etag", x.Etag.ToByteArray()},
                                                                           {"data", data}
                                                                       };
                                                      }));
                });
            }
            catch (InvalidDataException e)
            {
                RecordFailure(String.Empty, string.Format("Data is corrupted, could not proceed with attachment replication. Exception : {0}", e));
                scope.RecordError(e);
                log.ErrorException("Data is corrupted, could not proceed with replication", e);
            }
            catch (Exception e)
            {
                log.WarnException("Could not get attachments to replicate after: " + destinationsReplicationInformationForSource.LastAttachmentEtag, e);
            }
            return Tuple.Create(attachments, lastAttachmentEtag);
        }

        [Obsolete("Use RavenFS instead.")]
        private static List<AttachmentInformation> GetAttachmentsToReplicate(IStorageActionsAccessor actions, Etag lastAttachmentEtag, int? maxNumberOfItemsToReceiveInSingleBatch)
        {
            var maxNumberOfAttachments = 100;
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                maxNumberOfAttachments = Math.Min(maxNumberOfAttachments, maxNumberOfItemsToReceiveInSingleBatch.Value);

            var attachmentInformations = actions.Attachments.GetAttachmentsAfter(lastAttachmentEtag, maxNumberOfAttachments, 1024 * 1024 * 10).ToList();

            Etag lastEtag = null;
            if (attachmentInformations.Count > 0)
                lastEtag = attachmentInformations[attachmentInformations.Count - 1].Etag;

            var maxNumberOfTombstones = Math.Max(maxNumberOfAttachments, attachmentInformations.Count);
            var tombstones = actions
                .Lists
                .Read(Constants.RavenReplicationAttachmentsTombstones, lastAttachmentEtag, lastEtag, maxNumberOfTombstones + 1)
                            .Select(x => new AttachmentInformation
                            {
                                Key = x.Key,
                                Etag = x.Etag,
                                Metadata = x.Data,
                                Size = 0,
                            })
                .ToList();

            var results = attachmentInformations.Concat(tombstones);

            if (tombstones.Count >= maxNumberOfTombstones + 1)
            {
                var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
                log.Info("Replication batch trimmed. Found more than '{0}' attachment tombstones. Last attachment etag: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

                results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
            }

            results = results.OrderBy(x => x.Etag);

            // can't return earlier, because we need to know if there are tombstones that need to be send
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

            return results.ToList();
        }

        internal SourceReplicationInformationWithBatchInformation GetLastReplicatedEtagFrom(ReplicationStrategy destination)
        {
            try
            {
                Etag currentEtag = Etag.Empty;
                docDb.TransactionalStorage.Batch(accessor => currentEtag = accessor.Staleness.GetMostRecentDocumentEtag());
                var url = GetUrlFor(destination, "/replication/lastEtag");

                url += "&currentEtag=" + currentEtag;

                var request = httpRavenRequestFactory.Create(url, HttpMethods.Get, destination.ConnectionStringOptions);
                var lastReplicatedEtagFrom = request.ExecuteRequest<SourceReplicationInformationWithBatchInformation>();
                if (log.IsDebugEnabled)
                    log.Debug("Received last replicated document Etag {0} from server {1}", lastReplicatedEtagFrom.LastDocumentEtag, destination.ConnectionStringOptions.Url);
                return lastReplicatedEtagFrom;
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
                    log.WarnException("Replication is not enabled on: " + destination, e);
                else
                    log.WarnException("Failed to contact replication destination: " + destination, e);
                RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to contact replication destination: " + destination, e);
                RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
            }

            return null;
        }

        private string UrlEncodedServerUrl()
        {
            return Uri.EscapeDataString(docDb.ServerUrl);
        }

        internal ReplicationStrategy[] GetReplicationDestinations(Predicate<ReplicationDestination> predicate = null)
        {
            ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument;
            try
            {
                configurationDocument = docDb.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            }
            catch (Exception e)
            {
                log.Warn("Cannot get replication destinations", e);
                return new ReplicationStrategy[0];
            }

            if (configurationDocument == null)
            {
                return new ReplicationStrategy[0];
            }

            var replicationDocument = configurationDocument.MergedDocument;

            if (configurationDocument.LocalExists && string.IsNullOrWhiteSpace(replicationDocument.Source))
            {
                replicationDocument.Source = docDb.TransactionalStorage.Id.ToString();
                try
                {
                    var ravenJObject = RavenJObject.FromObject(replicationDocument);
                    ravenJObject.Remove("Id");
                    docDb.Documents.Put(Constants.RavenReplicationDestinations, configurationDocument.Etag, ravenJObject, configurationDocument.Metadata, null);
                }
                catch (ConcurrencyException)
                {
                    // we will get it next time
                }
            }

            if (replicationDocument.Source != docDb.TransactionalStorage.Id.ToString())
            {
                if (!wrongReplicationSourceAlertSent)
                {
                    var dbName = string.IsNullOrEmpty(docDb.Name) ? "<system>" : docDb.Name;

                    docDb.AddAlert(new Alert
                        {
                            AlertLevel = AlertLevel.Error,
                            CreatedAt = SystemTime.UtcNow,
                            Message = "Source of the ReplicationDestinations document is not the same as the database it is located in",
                            Title = "Wrong replication source: " + replicationDocument.Source + " instead of " + docDb.TransactionalStorage.Id + " in database " + dbName,
                            UniqueKey = "Wrong source: " + replicationDocument.Source + ", " + docDb.TransactionalStorage.Id
                        });

                    wrongReplicationSourceAlertSent = true;
                }

                return new ReplicationStrategy[0];
            }

            wrongReplicationSourceAlertSent = false;

            return replicationDocument
                .Destinations
                .Where(x => !x.Disabled)
                .Where(x => predicate == null || predicate(x))
                .Select(GetConnectionOptionsSafe)
                .Where(x => x != null)
                .ToArray();
        }

        private ReplicationStrategy GetConnectionOptionsSafe(ReplicationDestination x)
        {
            try
            {
                return GetConnectionOptions(x, docDb);
            }
            catch (Exception e)
            {
                log.ErrorException(
                    string.Format("IGNORING BAD REPLICATION CONFIG!{0}Could not figure out connection options for [Url: {1}, ClientVisibleUrl: {2}]",
                    Environment.NewLine, x.Url, x.ClientVisibleUrl),
                    e);

                return null;
            }
        }

        public static ReplicationStrategy GetConnectionOptions(ReplicationDestination destination, DocumentDatabase database)
        {
            var replicationStrategy = new ReplicationStrategy
            {
                ReplicationOptionsBehavior = destination.TransitiveReplicationBehavior,
                CurrentDatabaseId = database.TransactionalStorage.Id.ToString()
            };
            return CreateReplicationStrategyFromDocument(destination, replicationStrategy);
        }

        private static ReplicationStrategy CreateReplicationStrategyFromDocument(ReplicationDestination destination, ReplicationStrategy replicationStrategy)
        {
            var url = destination.Url;
            if (string.IsNullOrEmpty(destination.Database) == false)
            {
                url = url + "/databases/" + destination.Database;
            }

            replicationStrategy.ConnectionStringOptions = new RavenConnectionStringOptions
            {
                Url = url,
                ApiKey = destination.ApiKey,
            };

            if (destination.SpecifiedCollections != null)
            {
                replicationStrategy.SpecifiedCollections = new Dictionary<string, string>(destination.SpecifiedCollections, StringComparer.OrdinalIgnoreCase);
            }
            
            if (string.IsNullOrEmpty(destination.Username) == false)
            {
                replicationStrategy.ConnectionStringOptions.Credentials = string.IsNullOrEmpty(destination.Domain)
                    ? new NetworkCredential(destination.Username, destination.Password)
                    : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
            }

            return replicationStrategy;
        }

        public void HandleHeartbeat(string src)
        {
            ResetFailureForHeartbeat(src);

            heartbeatDictionary.AddOrUpdate(src, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);
        }

        public bool IsHeartbeatAvailable(string src, DateTime lastCheck)
        {
            if (heartbeatDictionary.ContainsKey(src))
            {
                DateTime lastHeartbeat;
                if (heartbeatDictionary.TryGetValue(src, out lastHeartbeat))
                {
                    return lastHeartbeat >= lastCheck;
                }
            }

            return false;
        }


        private void ResetFailureForHeartbeat(string src)
        {
            RecordSuccess(src, lastHeartbeatReceived: SystemTime.UtcNow);
            docDb.WorkContext.ShouldNotifyAboutWork(() => "Replication Heartbeat from " + src);
            docDb.WorkContext.NotifyAboutWork();
        }

        public void Dispose()
        {
            if (IndexReplication != null)
                IndexReplication.Dispose();

            if (TransformerReplication != null)
                TransformerReplication.Dispose();

            Task task;
            while (activeTasks.TryDequeue(out task))
            {
                task.Wait();
            }

            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                prefetchingBehavior.Value.Dispose();
            }
        }

        private readonly ConcurrentDictionary<string, DateTime> heartbeatDictionary = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        internal static void EnsureReplicationInformationInMetadata(RavenJObject metadata, DocumentDatabase database)
        {
            Debug.Assert(database != null);

            if (metadata == null)
                return;

            if (metadata.ContainsKey(Constants.RavenReplicationSource))
                return;

            metadata[Constants.RavenReplicationHistory] = new RavenJArray();
            metadata[Constants.RavenReplicationVersion] = 0;
            metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(database.TransactionalStorage.Id);
        }

        protected void OnReplicationExecuted()
        {
            var replicationExecuted = ReplicationExecuted;
            if (replicationExecuted != null) replicationExecuted();
        }

    }

    internal class ReplicationStatisticsRecorder : IDisposable
    {
        private readonly ReplicationStrategy destination;

        private readonly ConcurrentDictionary<string, DestinationStats> destinationStats;

        private readonly RavenJObject record;

        private readonly RavenJArray records;

        private readonly Stopwatch watch;

        public ReplicationStatisticsRecorder(ReplicationStrategy destination, ConcurrentDictionary<string, DestinationStats> destinationStats)
        {
            this.destination = destination;
            this.destinationStats = destinationStats;
            watch = Stopwatch.StartNew();
            Started = SystemTime.UtcNow;
            records = new RavenJArray();
            record = new RavenJObject
                     {
                         { "Url", destination.ConnectionStringOptions.Url },
                         { "StartTime", SystemTime.UtcNow},
                         { "Records", records }
                     };
        }

        public DateTime Started { get; private set; }


        public TimeSpan ElapsedTime
        {
            get
            {
                return watch.Elapsed;
            }
        }

        public void Dispose()
        {
            record.Add("TotalExecutionTime", watch.Elapsed.ToString());

            var stats = destinationStats.GetOrDefault(destination.ConnectionStringOptions.Url, new DestinationStats { Url = destination.ConnectionStringOptions.Url });

            stats.LastStats.Insert(0, record);

            while (stats.LastStats.Length > 50)
                stats.LastStats.RemoveAt(stats.LastStats.Length - 1);
        }

        public ReplicationStatisticsRecorderScope StartRecording(string name)
        {
            var scopeRecord = new RavenJObject();
            records.Add(scopeRecord);
            return new ReplicationStatisticsRecorderScope(name, scopeRecord);
        }

        internal class ReplicationStatisticsRecorderScope : IDisposable
        {
            private readonly RavenJObject record;

            private readonly RavenJArray records;

            private readonly Stopwatch watch;

            public ReplicationStatisticsRecorderScope(string name, RavenJObject record)
            {
                this.record = record;
                records = new RavenJArray();

                record.Add("Name", name);
                record.Add("Records", records);

                watch = Stopwatch.StartNew();
            }

            public void Dispose()
            {
                record.Add("ExecutionTime", watch.Elapsed.ToString());
            }

            public void Record(RavenJObject value)
            {
                records.Add(value);
            }

            public void RecordError(Exception exception)
            {
                records.Add(new RavenJObject
                            {
                                { "Error", new RavenJObject
                                           {
                                               { "Type", exception.GetType().Name }, 
                                               { "Message", exception.Message }
                                           } }
                            });
            }

            public void RecordError(string error)
            {
                records.Add(new RavenJObject
                            {
                                { "Error", error }
                            });
            }

            public ReplicationStatisticsRecorderScope StartRecording(string name)
            {
                var scopeRecord = new RavenJObject();
                records.Add(scopeRecord);
                return new ReplicationStatisticsRecorderScope(name, scopeRecord);
            }
        }
    }

}

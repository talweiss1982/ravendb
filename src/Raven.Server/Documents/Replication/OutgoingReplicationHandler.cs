﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        public const string AlertTitle = "Replication";

        internal readonly DocumentDatabase _database;
        internal readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly AsyncManualResetEvent _waitForChanges = new AsyncManualResetEvent();
        private readonly CancellationTokenSource _cts;
        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();
        private Thread _sendingThread;
        internal readonly DocumentReplicationLoader _parent;
        internal long _lastSentDocumentEtag;
        public long LastAcceptedDocumentEtag;
        internal long _lastSentIndexOrTransformerEtag;

        internal ReplicationStatistics.OutgoingBatchStats OutgoingStats = new ReplicationStatistics.OutgoingBatchStats();
        internal ReplicationStatistics ReplicationStats;

        internal DateTime _lastDocumentSentTime;
        internal DateTime _lastIndexOrTransformerSentTime;

        internal readonly Dictionary<Guid, long> _destinationLastKnownDocumentChangeVector =
            new Dictionary<Guid, long>();

        internal readonly Dictionary<Guid, long> _destinationLastKnownIndexOrTransformerChangeVector =
            new Dictionary<Guid, long>();

        internal string _destinationLastKnownDocumentChangeVectorAsString;
        internal string _destinationLastKnownIndexOrTransformerChangeVectorAsString;

        private TcpClient _tcpClient;

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        private JsonOperationContext.ManagedPinnedBuffer _buffer;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;
        private Stream _stream;
        private InterruptibleRead _interruptableRead;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

        public OutgoingReplicationHandler(DocumentReplicationLoader parent,
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _parent = parent;
            _database = database;
            _destination = destination;
            _log = LoggingSource.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _database.Changes.OnDocumentChange += OnDocumentChange;
            _database.Changes.OnIndexChange += OnIndexChange;
            _database.Changes.OnTransformerChange += OnTransformerChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            ReplicationStats = _parent.RepliactionStats;
        }

        public void Start()
        {
            _sendingThread = new Thread(ReplicateToDestination)
            {
                Name = $"Outgoing replication {FromToString}",
                IsBackground = true
            };
            _sendingThread.Start();
        }

        private TcpConnectionInfo GetTcpInfo()
        {
            JsonOperationContext context;
            using (var requestExecuter = RequestExecuter.ShortTermSingleUse(MultiDatabase.GetRootDatabaseUrl(_destination.Url), _destination.Database, _destination.ApiKey))
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new GetTcpInfoCommand();

                requestExecuter.Execute(command, context);

                var info = command.Result;
                if (_log.IsInfoEnabled)
                    _log.Info($"Will replicate to {_destination.Database} @ {_destination.Url} via {info.Url}");

                return info;
            }
        }

        private void ReplicateToDestination()
        {
            try
            {
                var connectionInfo = GetTcpInfo();

                using (_tcpClient = new TcpClient())
                {
                    ConnectSocket(connectionInfo, _tcpClient);

                    using (_stream = TcpUtils.WrapStreamWithSsl(_tcpClient,connectionInfo).Result)
                    using (_interruptableRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream))
                    using (_buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        var documentSender = new ReplicationDocumentSender(_stream, this, _log);
                        var indexAndTransformerSender = new ReplicationIndexTransformerSender(_stream, this, _log);

                        WriteHeaderToRemotePeer();
                        //handle initial response to last etag and staff
                        try
                        {
                            var response = HandleServerResponse(getFullResponse: true);
                            if (response.Item1 == ReplicationMessageReply.ReplyType.Error)
                            {
                                if (response.Item2.Exception.Contains("DatabaseDoesNotExistException"))
                                    throw new DatabaseDoesNotExistException(response.Item2.Message,
                                        new InvalidOperationException(response.Item2.Exception));
                                throw new InvalidOperationException(response.Item2.Exception);
                            }

                            if (response.Item1 == ReplicationMessageReply.ReplyType.Ok)
                            {
                                _parent.UpdateReplicationDocumentWithResolver(
                                    response.Item2.ResolverId,
                                    response.Item2.ResolverVersion);
                            }
                        }
                        catch (DatabaseDoesNotExistException e)
                        {
                            var msg =
                                $"Failed to parse initial server replication response, because there is no database named {_database.Name} on the other end. " +
                                "In order for the replication to work, a database with the same name needs to be created at the destination";
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(msg, e);
                            }

                            AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }
                        catch (Exception e)
                        {
                            var msg =
                                $"Failed to parse initial server response. This is definitely not supposed to happen.";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);

                            AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            long currentEtag;

                            Debug.Assert(_database.IndexMetadataPersistence.IsInitialized);

                            currentEtag = GetLastIndexEtag();

                            if (_destination.SkipIndexReplication == false &&
                                currentEtag != indexAndTransformerSender.LastEtag)
                            {
                                indexAndTransformerSender.ExecuteReplicationOnce();
                            }

                            var sp = Stopwatch.StartNew();
                            while (documentSender.ExecuteReplicationOnce())
                            {
                                if (sp.ElapsedMilliseconds > 60 * 1000)
                                {
                                    _waitForChanges.Set();
                                    break;
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (WaitForChanges(_parent.MinimalHeartbeatInterval, _cts.Token) == false)
                            {
                                SendHeartbeat();
                            }
                            _waitForChanges.Reset();
                        }
                    }
                }
            }
            catch (OperationCanceledException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Operation canceled on replication thread ({FromToString}). This is not necessary due to an issue. Stopped the thread.");
                Failed?.Invoke(this, e);
            }
            catch (IOException e)
            {
                if (_log.IsInfoEnabled)
                {
                    if (e.InnerException is SocketException)
                        _log.Info(
                            $"SocketException was thrown from the connection to remote node ({FromToString}). This might mean that the remote node is done or there is a network issue.",
                            e);
                    else
                        _log.Info($"IOException was thrown from the connection to remote node ({FromToString}).", e);
                }
                Failed?.Invoke(this, e);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Unexpected exception occured on replication thread ({FromToString}). Replication stopped (will be retried later).",
                        e);
                Failed?.Invoke(this, e);
            }
        }

        private long GetLastIndexEtag()
        {
            long currentEtag;
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (configurationContext.OpenReadTransaction())
                currentEtag =
                    _database.IndexMetadataPersistence.ReadLastEtag(configurationContext.Transaction.InnerTransaction);
            return currentEtag;
        }

        private void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (var txw = configurationContext.OpenWriteTransaction())
            {
                _database.NotificationCenter.AddAfterTransactionCommit(
                    AlertRaised.Create(AlertTitle, msg, AlertType.Replication, NotificationSeverity.Warning, key: FromToString, details: new ExceptionDetails(e)),
                    txw);

                txw.Commit();
            }
        }

        private void WriteHeaderToRemotePeer()
        {
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                var token = _authenticator.GetAuthenticationTokenAsync(_destination.ApiKey, _destination.ApiKey, documentsContext).Result;
                //send initial connection information
                documentsContext.Write(writer, new DynamicJsonValue
                {
                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _destination.Database,
                    [nameof(TcpConnectionHeaderMessage.Operation)] =
                    TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                    [nameof(TcpConnectionHeaderMessage.AuthorizationToken)] = token
                });
                writer.Flush();
                ReadHeaderResponseAndThrowIfUnAuthorized();
                //start request/response for fetching last etag
                var request = new DynamicJsonValue
                {
                    ["Type"] = "GetLastEtag",
                    ["SourceDatabaseId"] = _database.DbId.ToString(),
                    ["SourceDatabaseName"] = _database.Name,
                    ["SourceUrl"] = _database.Configuration.Core.ServerUrl,
                    ["MachineName"] = Environment.MachineName,
                    ["ResolverVersion"] = _parent?.ReplicationDocument.DefaultResolver?.Version,
                    ["ResolverId"] = _parent?.ReplicationDocument.DefaultResolver?.ResolvingDatabaseId,
                };

                documentsContext.Write(writer, request);
                writer.Flush();
            }
        }

        private void ReadHeaderResponseAndThrowIfUnAuthorized()
        {
            var timeout = 2 * 60 * 1000; // TODO: configurable
            using (var replicationTcpConnectReplyMessage = _interruptableRead.ParseToMemory(
                _connectionDisposed,
                "replication acknowledge response",
                timeout,
                _buffer,
                CancellationToken))
            {
                if (replicationTcpConnectReplyMessage.Timeout)
                {
                    ThrowTimeout(timeout);
                }
                if (replicationTcpConnectReplyMessage.Interrupted)
                {
                    ThrowConnectionClosed();
                }
                var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(replicationTcpConnectReplyMessage.Document);
                switch (headerResponse.Status)
                {
                    case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                        //All good nothing to do
                        break;
                    default:
                        throw new UnauthorizedAccessException($"{_destination.Url}/{_destination.Database} replied with failure {headerResponse.Status}");
                }
            }
        }

        private bool WaitForChanges(int timeout, CancellationToken token)
        {
            while (true)
            {
                using (var result = _interruptableRead.ParseToMemory(
                    _waitForChanges,
                    "replication notify message",
                    timeout,
                    _buffer,
                    token))
                {
                    if (result.Document != null)
                    {
                        HandleServerResponse(result.Document, allowNotify: true);
                    }
                    else
                    {
                        return result.Timeout == false;
                    }
                }
            }
        }

        internal void WriteToServer(DynamicJsonValue val)
        {
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, val);
            }
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            if (replicationBatchReply.MessageType == null)
                throw new InvalidOperationException(
                    "MessageType on replication response is null. This is likely is a symptom of an issue, and should be investigated.");

            _destinationLastKnownDocumentChangeVector.Clear();

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        private void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = Math.Max(_lastSentDocumentEtag, replicationBatchReply.LastEtagAccepted);
            _lastSentIndexOrTransformerEtag = Math.Max(_lastSentIndexOrTransformerEtag,
                replicationBatchReply.LastIndexTransformerEtagAccepted);
            LastAcceptedDocumentEtag = replicationBatchReply.LastEtagAccepted;

            _destinationLastKnownDocumentChangeVectorAsString = replicationBatchReply.DocumentsChangeVector.Format();
            _destinationLastKnownIndexOrTransformerChangeVectorAsString =
                replicationBatchReply.IndexTransformerChangeVector.Format();

            foreach (var changeVectorEntry in replicationBatchReply.DocumentsChangeVector)
            {
                _destinationLastKnownDocumentChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }

            foreach (var changeVectorEntry in replicationBatchReply.IndexTransformerChangeVector)
            {
                _destinationLastKnownIndexOrTransformerChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }

            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (documentsContext.OpenReadTransaction())
            {
                if (DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastEtagAccepted)
                {
                    // We have changes that the other side doesn't have, this can be because we have writes
                    // or because we have documents that were replicated to us. Either way, we need to sync
                    // those up with the remove side, so we'll start the replication loop again.
                    // We don't care if they are locally modified or not, because we filter documents that
                    // the other side already have (based on the change vector).
                    if ((DateTime.UtcNow - _lastDocumentSentTime).TotalMilliseconds > _parent.MinimalHeartbeatInterval)
                        _waitForChanges.SetByAsyncCompletion();
                }
            }
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (configurationContext.OpenReadTransaction())
            {
                if (
                    _database.IndexMetadataPersistence.ReadLastEtag(configurationContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastIndexTransformerEtagAccepted)
                {
                    if ((DateTime.UtcNow - _lastIndexOrTransformerSentTime).TotalMilliseconds >
                        _parent.MinimalHeartbeatInterval)
                        _waitForChanges.SetByAsyncCompletion();
                }
            }
        }

        public string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

        public ReplicationDestination Destination => _destination;

        internal void SendHeartbeat()
        {
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                try
                {
                    var heartbeat = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastSentDocumentEtag,
                        [nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag)] = _lastSentIndexOrTransformerEtag,
                        [nameof(ReplicationMessageHeader.ItemCount)] = 0
                    };
                    documentsContext.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                    throw;
                }

                try
                {
                    HandleServerResponse();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Parsing heartbeat result failed. ({FromToString})", e);
                    throw;
                }
            }
        }

        internal Tuple<ReplicationMessageReply.ReplyType, ReplicationMessageReply> HandleServerResponse(bool getFullResponse = false)
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000;// TODO: configurable
                using (var replicationBatchReplyMessage = _interruptableRead.ParseToMemory(
                    _connectionDisposed,
                    "replication acknowledge message",
                    timeout,
                    _buffer,
                    CancellationToken))
                {
                    if (replicationBatchReplyMessage.Timeout)
                    {
                        ThrowTimeout(timeout);
                    }
                    if (replicationBatchReplyMessage.Interrupted)
                    {
                        ThrowConnectionClosed();
                    }

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage.Document,
                        allowNotify: false);
                    if (replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

                    var sendFullReply = replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error ||
                                        getFullResponse;

                    return Tuple.Create(replicationBatchReply.Type, sendFullReply ? replicationBatchReply : null);
                }
            }
        }

        private static void ThrowTimeout(int timeout)
        {
            throw new TimeoutException("Could not get a server response in a reasonable time " +
                                       TimeSpan.FromMilliseconds(timeout));
        }

        private static void ThrowConnectionClosed()
        {
            throw new OperationCanceledException("The connection has been closed by the Dispose method");
        }

        internal ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage,
            bool allowNotify)
        {
            replicationBatchReplyMessage.BlittableValidation();
            var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);
            if (allowNotify == false && replicationBatchReply.MessageType == "Notify")
                return null;

            DestinationDbId = replicationBatchReply.DatabaseId;

            switch (replicationBatchReply.Type)
            {
                case ReplicationMessageReply.ReplyType.Ok:
                    UpdateDestinationChangeVector(replicationBatchReply);
                    OnSuccessfulTwoWaysCommunication();
                    break;
                default:
                    var msg =
                        $"Received error from remote replication destination. Error received: {replicationBatchReply.Exception}";
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(msg);
                    }
                    break;
            }

            if (_log.IsInfoEnabled)
            {
                switch (replicationBatchReply.Type)
                {
                    case ReplicationMessageReply.ReplyType.Ok:
                        _log.Info(
                            $"Received reply for replication batch from {_destination.Database} @ {_destination.Url}. New destination change vector is {_destinationLastKnownDocumentChangeVectorAsString}");
                        break;
                    case ReplicationMessageReply.ReplyType.Error:
                        _log.Info(
                            $"Received reply for replication batch from {_destination.Database} at {_destination.Url}. There has been a failure, error string received : {replicationBatchReply.Exception}");
                        throw new InvalidOperationException(
                            $"Received failure reply for replication batch. Error string received = {replicationBatchReply.Exception}");
                    default:
                        throw new ArgumentOutOfRangeException(nameof(replicationBatchReply),
                            "Received reply for replication batch with unrecognized type... got " +
                            replicationBatchReply.Type);
                }
            }
            return replicationBatchReply;
        }

        private void ConnectSocket(TcpConnectionInfo connection, TcpClient tcpClient)
        {
            var uri = new Uri(connection.Url);
            var host = uri.Host;
            var port = uri.Port;

            try
            {
                tcpClient.ConnectAsync(host, port).Wait(CancellationToken);
            }
            catch (SocketException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Failed to connect to remote replication destination {connection.Url}. Socket Error Code = {e.SocketErrorCode}",
                        e);
                throw;
            }
            catch (OperationCanceledException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $@"Tried to connect to remote replication destination {connection.Url}, but the operation was aborted. 
                            This is not necessarily an issue, it might be that replication destination document has changed at 
                            the same time we tried to connect. We will try to reconnect later.",
                        e);
                throw;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {connection.Url}", e);
                throw;
            }
        }

        private void OnDocumentChange(DocumentChange change)
        {
            if (change.TriggeredByReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private void OnIndexChange(IndexChange change)
        {
            if (change.Type != IndexChangeTypes.IndexAdded &&
                change.Type != IndexChangeTypes.IndexRemoved)
                return;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Received index {change.Type} event, index name = {change.Name}, etag = {change.Etag}");

            if (IncomingReplicationHandler.IsIncomingReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private void OnTransformerChange(TransformerChange change)
        {
            if (change.Type != TransformerChangeTypes.TransformerAdded &&
                change.Type != TransformerChangeTypes.TransformerRemoved)
                return;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Received transformer {change.Type} event, transformer name = {change.Name}, etag = {change.Etag}");

            if (IncomingReplicationHandler.IsIncomingReplicationThread)
                return;
            _waitForChanges.Set();
        }

        public void Dispose()
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString})");

            _database.Changes.OnDocumentChange -= OnDocumentChange;
            _database.Changes.OnIndexChange -= OnIndexChange;
            _database.Changes.OnTransformerChange -= OnTransformerChange;

            _cts.Cancel();

            try
            {
                _tcpClient?.Dispose();
            }
            catch (Exception) { }

            _connectionDisposed.Set();

            if (_sendingThread != Thread.CurrentThread)
            {
                _sendingThread?.Join();
            }
        }

        private void OnSuccessfulTwoWaysCommunication() => SuccessfulTwoWaysCommunication?.Invoke(this);

    }

    public static class ReplicationMessageType
    {
        public const string Heartbeat = "Heartbeat";
        public const string IndexesTransformers = "IndexesTransformers";
        public const string Documents = "Documents";
    }
}
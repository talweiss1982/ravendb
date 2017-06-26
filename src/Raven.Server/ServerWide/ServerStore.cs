﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Util;
using Raven.Client.Exceptions.Server;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Client.Server.Operations;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private StorageEnvironment _env;

        private readonly NotificationsStorage _notificationsStorage;
        private readonly OperationsStorage _operationsStorage;

        private RequestExecutor _clusterRequestExecutor;

        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _ravenServer;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly NotificationCenter.NotificationCenter NotificationCenter;
        public readonly LicenseManager LicenseManager;
        public readonly FeedbackSender FeedbackSender;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        public Operations Operations { get; }

        public ServerStore(RavenConfiguration configuration, RavenServer ravenServer)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            _ravenServer = ravenServer;

            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage(ResourceName);

            NotificationCenter = new NotificationCenter.NotificationCenter(_notificationsStorage, ResourceName, ServerShutdown);

            _operationsStorage = new OperationsStorage();

            Operations = new Operations(ResourceName, _operationsStorage, NotificationCenter, null);

            LicenseManager = new LicenseManager(NotificationCenter);

            FeedbackSender = new FeedbackSender();

            DatabaseInfoCache = new DatabaseInfoCache();

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;

        }

        public RavenServer RavenServer => _ravenServer;

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

        public long LastRaftCommitEtag
        {
            get
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    return _engine.GetLastCommitIndex(context);
            }
        }

        public ClusterStateMachine Cluster => _engine.StateMachine;
        public string LeaderTag => _engine.LeaderTag;
        public RachisConsensus.State CurrentState => _engine.CurrentState;
        public string NodeTag => _engine.Tag;

        public bool Disposed => _disposed;

        private Timer _timer;
        private RachisConsensus<ClusterStateMachine> _engine;
        private bool _disposed;
        public RachisConsensus<ClusterStateMachine> Engine => _engine;

        public ClusterMaintenanceSupervisor ClusterMaintenanceSupervisor;

        public Dictionary<string, ClusterNodeStatusReport> ClusterStats()
        {
            if (_engine.LeaderTag != NodeTag)
                throw new NotLeadingException($"Stats can be requested only from the raft leader {_engine.LeaderTag}");
            return ClusterMaintenanceSupervisor?.GetStats();
        }

        public async Task ClusterMaintenanceSetupTask()
        {
            while (true)
            {
                try
                {
                    if (_engine.LeaderTag != NodeTag)
                    {
                        await _engine.WaitForState(RachisConsensus.State.Leader)
                            .WithCancellation(_shutdownNotification.Token);
                        continue;
                    }
                    using (ClusterMaintenanceSupervisor = new ClusterMaintenanceSupervisor(this, _engine.Tag, _engine.CurrentTerm))
                    using (new ClusterObserver(this, ClusterMaintenanceSupervisor, _engine, ContextPool, ServerShutdown))
                    {
                        var oldNodes = new Dictionary<string, string>();
                        while (_engine.LeaderTag == NodeTag)
                        {
                            var topologyChangedTask = _engine.GetTopologyChanged();
                            ClusterTopology clusterTopology;
                            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            using (context.OpenReadTransaction())
                            {
                                clusterTopology = _engine.GetTopology(context);
                            }
                            var newNodes = clusterTopology.AllNodes;
                            var nodesChanges = ClusterTopology.DictionaryDiff(oldNodes, newNodes);
                            oldNodes = newNodes;
                            foreach (var node in nodesChanges.removedValues)
                            {
                                ClusterMaintenanceSupervisor.RemoveFromCluster(node.Key);
                            }
                            foreach (var node in nodesChanges.addedValues)
                            {
                                var task = ClusterMaintenanceSupervisor.AddToCluster(node.Key, clusterTopology.GetUrlFromTag(node.Key)).ContinueWith(t =>
                                {
                                    if (Logger.IsInfoEnabled)
                                        Logger.Info($"ClusterMaintenanceSupervisor() => Failed to add to cluster node key = {node.Key}", t.Exception);
                                }, TaskContinuationOptions.OnlyOnFaulted);
                                GC.KeepAlive(task);
                            }

                            var leaderChanged = _engine.WaitForLeaveState(RachisConsensus.State.Leader);
                            if (await Task.WhenAny(topologyChangedTask, leaderChanged)
                                    .WithCancellation(_shutdownNotification.Token) == leaderChanged)
                                break;
                        }
                    }
                }
                catch (TaskCanceledException)
                {
// ServerStore dispose?
                    throw;
                }
                catch (Exception)
                {
                    //
                }
            }
        }

        public ClusterTopology GetClusterTopology(TransactionOperationContext context)
        {
            return _engine.GetTopology(context);
        }

        public async Task AddNodeToClusterAsync(string nodeUrl, byte[] publicKey, string nodeTag = null, bool validateNotInTopology = true)
        {
            await _engine.AddToClusterAsync(nodeUrl, publicKey, nodeTag, validateNotInTopology).WithCancellation(_shutdownNotification.Token);
        }

        public async Task RemoveFromClusterAsync(string nodeTag)
        {
            await _engine.RemoveFromClusterAsync(nodeTag).WithCancellation(_shutdownNotification.Token);
        }

        public void Initialize()
        {
            LowMemoryNotification.Initialize(ServerShutdown,
                Configuration.Memory.LowMemoryDetection.GetValue(SizeUnit.Bytes),
                Configuration.Memory.PhysicalRatioForLowMemDetection);

            if (Logger.IsInfoEnabled)
                Logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");
            var storeAlertForLateRaise = new List<AlertRaised>();

            StorageEnvironmentOptions options;
            if (Configuration.Core.RunInMemory)
            {
                options = StorageEnvironmentOptions.CreateMemoryOnly();
            }
            else
            {
                options = StorageEnvironmentOptions.ForPath(path.FullPath);
                var secretKey = Path.Combine(path.FullPath, "secret.key.encrypted");
                if (File.Exists(secretKey))
                {
                    byte[] buffer;
                    try
                    {
                        buffer = File.ReadAllBytes(secretKey);
                    }
                    catch (Exception e)
                    {
                        throw new FileLoadException($"The server store secret key is provided in {secretKey} but the server failed to read the file. Admin assistance required.", e);
                    }
                    
                    var secret = new byte[buffer.Length - 32];
                    var entropy = new byte[32];
                    Array.Copy(buffer, 0, secret, 0, buffer.Length - 32);
                    Array.Copy(buffer, buffer.Length - 32, entropy, 0, 32);

                    try
                    {
                        options.MasterKey = SecretProtection.Unprotect(secret, entropy);
                    }
                    catch (Exception e)
                    {
                        throw new CryptographicException($"Unable to unprotect the secret key file {secretKey}. " +
                                                         $"Was the server store encrypted using a different OS user? In that case, " +
                                                         $"you must provide an unprotected key (rvn server put-key). " +
                                                         $"Admin assistance required.", e);
                    }
                }
            }

            options.OnNonDurableFileSystemError += (obj, e) =>
            {
                var alert = AlertRaised.Create("Non Durable File System - System Storage",
                    e.Message,
                    AlertType.NonDurableFileSystem,
                    NotificationSeverity.Warning,
                    "NonDurable Error System",
                    details: new MessageDetails {Message = e.Details});
                if (NotificationCenter.IsInitialized)
                {
                    NotificationCenter.Add(alert);
                }
                else
                {
                    storeAlertForLateRaise.Add(alert);
                }
            };

            options.OnRecoveryError += (obj, e) =>
            {
                var alert = AlertRaised.Create("Recovery Error - System Storage",
                    e.Message,
                    AlertType.NonDurableFileSystem,
                    NotificationSeverity.Error,
                    "Recovery Error System");
                if (NotificationCenter.IsInitialized)
                {
                    NotificationCenter.Add(alert);
                }
                else
                {
                    storeAlertForLateRaise.Add(alert);
                }
            };

            try
            {
                if (MemoryInformation.IsSwappingOnHddInsteadOfSsd())
                {
                    var alert = AlertRaised.Create("Swap Storage Type Warning",
                        "OS swapping on at least one HDD drive while there is at least one SSD drive on this system. " +
                        "This can cause a slowdown, consider moving swap-partition/pagefile to SSD",
                        AlertType.SwappingHddInsteadOfSsd,
                        NotificationSeverity.Warning);
                    if (NotificationCenter.IsInitialized)
                    {
                        NotificationCenter.Add(alert);
                    }
                    else
                    {
                        storeAlertForLateRaise.Add(alert);
                    }
                }
            }
            catch (Exception e)
            {
                // the above should not throw, but we mask it in case it does (as it reads IO parameters) - this alert is just a nice-to-have warning
                if (Logger.IsInfoEnabled)
                    Logger.Info("An error occurred while trying to determine Is Swapping On Hdd Instead Of Ssd", e);
            }

            options.SchemaVersion = 2;
            options.ForceUsing32BitsPager = Configuration.Storage.ForceUsing32BitsPager;
            try
            {
                StorageEnvironment.MaxConcurrentFlushes = Configuration.Storage.MaxConcurrentFlushes;

                try
                {
                    _env = new StorageEnvironment(options);
                }
                catch (Exception e)
                {
                    throw new ServerLoadFailureException("Failed to load system storage " + Environment.NewLine + $"At {options.BasePath}", e);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(
                        "Could not open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath), e);
                options.Dispose();
                throw;
            }

            if (Configuration.Queries.MaxClauseCount != null)
                BooleanQuery.MaxClauseCount = Configuration.Queries.MaxClauseCount.Value;

            ContextPool = new TransactionContextPool(_env);


            _engine = new RachisConsensus<ClusterStateMachine>();

            var myUrl = Configuration.Core.PublicServerUrl.HasValue ? Configuration.Core.PublicServerUrl.Value.UriValue : Configuration.Core.ServerUrl;
            _engine.Initialize(_env, Configuration.Cluster, myUrl);

            _engine.StateMachine.DatabaseChanged += DatabasesLandlord.ClusterOnDatabaseChanged;
            _engine.StateMachine.DatabaseChanged += OnDatabaseChanged;
            _engine.StateMachine.DatabaseValueChanged += DatabasesLandlord.ClusterOnDatabaseValueChanged;

            _engine.TopologyChanged += OnTopologyChanged;
            _engine.StateChanged += OnStateChanged;

            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            _notificationsStorage.Initialize(_env, ContextPool);
            _operationsStorage.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);

            NotificationCenter.Initialize();
            foreach (var alertRaised in storeAlertForLateRaise)
            {
                NotificationCenter.Add(alertRaised);
            }
            LicenseManager.Initialize(_env, ContextPool);
            LatestVersionCheck.Check(this);

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                foreach (var db in _engine.StateMachine.ItemsStartingWith(context, "db/", 0, int.MaxValue))
                {
                    DatabasesLandlord.ClusterOnDatabaseChanged(this, (db.Item1, 0, "Init"));
                }
            }

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                if (TryLoadAuthenticationKeyPairs(ctx) == false)
                    GenerateAuthenticationSignetureKeys(ctx);                
            }
            GenerateAuthenticationBoxKeys();

            Task.Run(ClusterMaintenanceSetupTask, ServerShutdown);
        }

        public byte[] BoxPublicKey, BoxSecretKey, SignPublicKey, SignSecretKey;


        internal string GetClusterTokenForNode(JsonOperationContext context)
        {
            return System.Text.Encoding.UTF8.GetString(SignedTokenGenerator.GenerateToken(context, SignSecretKey,
                Constants.ApiKeys.ClusterApiKeyName, NodeTag).ToArray());
        }

        private void GenerateAuthenticationSignetureKeys(TransactionOperationContext ctx)
        {
            SignPublicKey = new byte[Sodium.crypto_sign_publickeybytes()];
            SignSecretKey = new byte[Sodium.crypto_sign_secretkeybytes()];
            unsafe
            {
                fixed (byte* signPk = SignPublicKey)
                fixed (byte* signSk = SignSecretKey)
                {
                    if (Sodium.crypto_sign_keypair(signPk, signSk) != 0)
                        throw new CryptographicException("Unable to generate crypto_sign_keypair for authentication");
                }
            }

            using (var tx = ctx.OpenWriteTransaction())
            {
                PutSecretKey(ctx, "Raven/Sign/Public", SignPublicKey, overwrite: true, cloneKey: true);
                PutSecretKey(ctx, "Raven/Sign/Private", SignSecretKey, overwrite: true, cloneKey: true);
                tx.Commit();
            }
        }
        private void GenerateAuthenticationBoxKeys()
        {
            BoxPublicKey = new byte[Sodium.crypto_box_publickeybytes()];
            BoxSecretKey = new byte[Sodium.crypto_box_secretkeybytes()];
            unsafe
            {
                fixed (byte* boxPk = BoxPublicKey)
                fixed (byte* boxSk = BoxSecretKey)
                {
                    if (Sodium.crypto_box_keypair(boxPk, boxSk) != 0)
                        throw new CryptographicException("Unable to generate crypto_box_keypair for authentication");
                }
            }

        }

        private bool TryLoadAuthenticationKeyPairs(TransactionOperationContext ctx)
        {
            using (ctx.OpenReadTransaction())
            {
                SignPublicKey = GetSecretKey(ctx, "Raven/Sign/Public");
                SignSecretKey = GetSecretKey(ctx, "Raven/Sign/Private");
            }

            return BoxPublicKey != null && BoxSecretKey != null && SignPublicKey != null && SignSecretKey != null;
        }

        private void OnStateChanged(object sender, RachisConsensus.StateTransition state)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                NotificationCenter.Add(ClusterTopologyChanged.Create(GetClusterTopology(context), LeaderTag, NodeTag));
                // If we are in passive state, we prevent from tasks to be performed by this node.
                if (state.From == RachisConsensus.State.Passive || state.To == RachisConsensus.State.Passive)
                {
                    RefreshOutgoingTasks();
                }
            }
        }

        public Task RefreshOutgoingTasks()
        {
            return RefreshOutgoingTasksAsync();
        }

        public async Task RefreshOutgoingTasksAsync()
        {
            var tasks = new Dictionary<string, Task<DocumentDatabase>>();
            foreach (var db in DatabasesLandlord.DatabasesCache)
            {
                tasks.Add(db.Key, db.Value);
            }
            while (tasks.Any())
            {
                var completedTask = await Task.WhenAny(tasks.Values);
                var name = tasks.Single(t => t.Value == completedTask).Key;
                tasks.Remove(name);
                try
                {
                    var database = await completedTask;
                    database.RefreshFeatures();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"An error occured while disabling outgoing tasks on the database {name}", e);
                    }
                }
            }
        }

        private void OnTopologyChanged(object sender, ClusterTopology topologyJson)
        {
            NotificationCenter.Add(ClusterTopologyChanged.Create(topologyJson, LeaderTag, NodeTag));
        }

        private void OnDatabaseChanged(object sender, (string dbName, long index, string type) t)
        {
            switch (t.type)
            {
                case nameof(DeleteDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.dbName, DatabaseChangeType.Delete));
                    break;
                case nameof(AddDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.dbName, DatabaseChangeType.Put));
                    break;
                case nameof(UpdateTopologyCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.dbName, DatabaseChangeType.Update));
                    break;
            }

            //TODO: send different commands to studio when necessary
        }

        public IEnumerable<string> GetSecretKeysNames(TransactionOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree("SecretKeys");
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {

                    yield return it.CurrentKey.ToString();

                } while (it.MoveNext());
            }

        }

        public unsafe void PutSecretKey(string base64, string name, bool overwrite)
        {
            var key = Convert.FromBase64String(base64);
            if (key.Length != 256 / 8)
                throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

            fixed (char* pBase64 = base64)
            fixed (byte* pKey = key)
            {
                try
                {
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        PutSecretKey(ctx, name, key, overwrite);
                        tx.Commit();
                    }
                }
                finally
                {
                    Sodium.ZeroMemory((byte*)pBase64, base64.Length * sizeof(char));
                    Sodium.ZeroMemory(pKey, key.Length);
                }
            }
        }

        public unsafe void PutSecretKey(
            TransactionOperationContext context,
            string name,
            byte[] secretKey,
            bool overwrite = false, /* Be careful with this one, overwriting a key might be disastrous */
            bool cloneKey = false)
        {
            Debug.Assert(context.Transaction != null);
            if (secretKey.Length != 256 / 8 && secretKey.Length != 512 / 8)
                throw new ArgumentException($"Key size must be 256 bits or 512 bits, but was {secretKey.Length * 8}", nameof(secretKey));

            byte[] key;
            if (cloneKey)
                Sodium.CloneKey(out key, secretKey);
            else
                key = secretKey;
            
            
            byte[] existingKey;
            try
            {
                existingKey = GetSecretKey(context, name);
            }
            catch (Exception)
            {
                // failure to read the key might be because the user password has changed
                // in this case, we ignore the existence of the key and overwrite it
                existingKey = null;
            }
            if (existingKey != null)
            {
                fixed (byte* pKey = key)
                fixed (byte* pExistingKey = existingKey)
                {
                    bool areEqual = Sparrow.Memory.Compare(pKey, pExistingKey, key.Length) == 0;
                    Sodium.ZeroMemory(pExistingKey, key.Length);
                    if (areEqual)
                    {
                        Sodium.ZeroMemory(pKey, key.Length);
                        return;
                    }
                }
            }

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");
            var record = Cluster.ReadDatabase(context, name);

            if (overwrite == false && tree.Read(name) != null)
                throw new InvalidOperationException($"Attempt to overwrite secret key {name}, which isn\'t permitted (you\'ll lose access to the encrypted db).");

            if (record != null && record.Encrypted == false)
                throw new InvalidOperationException($"Cannot modify key {name} where there is an existing database that is not encrypted");

            var hashLen = Sodium.crypto_generichash_bytes_max();
            var hash = new byte[hashLen + key.Length];
            fixed (byte* pHash = hash)
            fixed (byte* pKey = key)
            {
                try
                {
                    if (Sodium.crypto_generichash(pHash, (UIntPtr)hashLen, pKey, (ulong)key.Length, null, UIntPtr.Zero) != 0)
                        throw new InvalidOperationException("Failed to hash key");

                    Sparrow.Memory.Copy(pHash + hashLen, pKey, key.Length);

                    var entropy = Sodium.GenerateRandomBuffer(256);

                    var protectedData = SecretProtection.Protect(hash, entropy);

                    var ms = new MemoryStream();
                    ms.Write(entropy, 0, entropy.Length);
                    ms.Write(protectedData, 0, protectedData.Length);
                    ms.Position = 0;

                    tree.Add(name, ms);
                }
                finally
                {
                    Sodium.ZeroMemory(pHash, hash.Length);
                    Sodium.ZeroMemory(pKey, key.Length);
                }
            }
        }


        public unsafe byte[] GetSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            var tree = context.Transaction.InnerTransaction.ReadTree("SecretKeys");

            var readResult = tree?.Read(name);
            if (readResult == null)
                return null;

            const int numberOfBits = 256;
            var entropy = new byte[numberOfBits / 8];
            var reader = readResult.Reader;
            reader.Read(entropy, 0, entropy.Length);
            var protectedData = new byte[reader.Length - entropy.Length];
            reader.Read(protectedData, 0, protectedData.Length);

            var data = SecretProtection.Unprotect(protectedData, entropy);

            var hashLen = Sodium.crypto_generichash_bytes_max();

            fixed (byte* pData = data)
            fixed (byte* pHash = new byte[hashLen])
            {
                try
                {
                    if (Sodium.crypto_generichash(pHash, (UIntPtr)hashLen, pData + hashLen, (ulong)(data.Length - hashLen), null, UIntPtr.Zero) != 0)
                        throw new InvalidOperationException($"Unable to compute hash for {name}");

                    if (Sodium.sodium_memcmp(pData, pHash, (UIntPtr)hashLen) != 0)
                        throw new InvalidOperationException($"Unable to validate hash after decryption for {name}, user store changed?");

                    var buffer = new byte[data.Length - hashLen];
                    fixed (byte* pBuffer = buffer)
                    {
                        Sparrow.Memory.Copy(pBuffer, pData + hashLen, buffer.Length);
                    }
                    return buffer;
                }
                finally
                {
                    Sodium.ZeroMemory(pData, data.Length);
                }
            }
        }

        public void DeleteSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");

            tree.Delete(name);
        }

        public Task<(long Etag, object Result)> DeleteDatabaseAsync(string db, bool hardDelete, string fromNode)
        {
            var deleteCommand = new DeleteDatabaseCommand(db)
            {
                HardDelete = hardDelete,
                FromNode = fromNode
            };
            return SendToLeaderAsync(deleteCommand);
        }

        public Task<(long Etag, object Result)> ModifyCustomFunctions(string dbName, string customFunctions)
        {
            var customFunctionsCommand = new Commands.ModifyCustomFunctionsCommand(dbName)
            {
                CustomFunctions = customFunctions
            };
            return SendToLeaderAsync(customFunctionsCommand);
        }

        public Task<(long Etag, object Result)> UpdateExternalReplication(string dbName, ExternalReplication watcher)
        {
            var addWatcherCommand = new UpdateExternalReplicationCommand(dbName)
            {
                Watcher = watcher
            };
            return SendToLeaderAsync(addWatcherCommand);
        }

        public Task<(long Etag, object Result)> DeleteOngoingTask(long taskId, OngoingTaskType taskType, string dbName)
        {
            var deleteTaskCommand = new DeleteOngoingTaskCommand(taskId, taskType, dbName);

            return SendToLeaderAsync(deleteTaskCommand);
        }

        public Task<(long Etag, object Result)> ToggleTaskState(long taskId, OngoingTaskType type, bool disable, string dbName)
        {
            var disableEnableCommand = new ToggleTaskStateCommand(taskId, type, disable, dbName);

            return SendToLeaderAsync(disableEnableCommand);
        }

        public Task<(long Etag, object Result)> ModifyConflictSolverAsync(string dbName, ConflictSolver solver)
        {
            var conflictResolverCommand = new ModifyConflictSolverCommand(dbName)
            {
                Solver = solver
            };
            return SendToLeaderAsync(conflictResolverCommand);
        }

        public Task<(long Etag, object Result)> PutValueInClusterAsync<T>(PutValueCommand<T> cmd)
        {
            return SendToLeaderAsync(cmd);
        }

        public Task<(long Etag, object Result)> DeleteValueInClusterAsync(string key)
        {
            var deleteValueCommand = new DeleteValueCommand
            {
                Name = key
            };
            return SendToLeaderAsync(deleteValueCommand);
        }

        public Task<(long Etag, object Result)> ModifyDatabaseExpiration(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var editExpiration = new EditExpirationCommand(JsonDeserializationCluster.ExpirationConfiguration(configurationJson), name);
            return SendToLeaderAsync(editExpiration);
        }

        public async Task<(long, object)> ModifyPeriodicBackup(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var modifyPeriodicBackup = new UpdatePeriodicBackupCommand(JsonDeserializationCluster.PeriodicBackupConfiguration(configurationJson), name);
            return await SendToLeaderAsync(modifyPeriodicBackup);
        }

        public async Task<(long, object)> AddEtl(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject etlConfiguration, EtlType type)
        {
            UpdateDatabaseCommand command;

            switch (type)
            {
                case EtlType.Raven:
                    command = new AddRavenEtlCommand(JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration), databaseName);
                    break;
                case EtlType.Sql:
                    command = new AddSqlEtlCommand(JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration), databaseName);
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration destination type: {type}");
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> UpdateEtl(TransactionOperationContext context, string databaseName, long id, BlittableJsonReaderObject etlConfiguration,
            EtlType type)
        {
            UpdateDatabaseCommand command;

            switch (type)
            {
                case EtlType.Raven:
                    command = new UpdateRavenEtlCommand(id, JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration), databaseName);
                    break;
                case EtlType.Sql:
                    command = new UpdateSqlEtlCommand(id, JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration), databaseName);
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration destination type: {type}");
            }

            return await SendToLeaderAsync(command);
        }

        public Task<(long, object)> ModifyDatabaseVersioning(JsonOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var editVersioning = new EditVersioningCommand(JsonDeserializationCluster.VersioningConfiguration(configurationJson), name);
            return SendToLeaderAsync(editVersioning);
        }

        public async Task<(long, object)> AddConnectionString(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject connectionString)
        {
            if (connectionString.TryGet(nameof(ConnectionString.Type), out string type) == false)
                throw new InvalidOperationException($"Connection string must have {nameof(ConnectionString.Type)} field");

            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

            UpdateDatabaseCommand command;
            
            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    command = new AddRavenConnectionString(JsonDeserializationCluster.RavenConnectionString(connectionString), databaseName);
                    break;
                case ConnectionStringType.Sql:
                    command = new AddSqlConnectionString(JsonDeserializationCluster.SqlConnectionString(connectionString), databaseName);
                    break;
                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return await SendToLeaderAsync(command);
        }

        public Guid GetServerId()
        {
            return _env.DbId;
        }

        public void Dispose()
        {
            if (_shutdownNotification.IsCancellationRequested || _disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                try
                {
                    if (_shutdownNotification.IsCancellationRequested)
                        return;

                    _shutdownNotification.Cancel();
                    var toDispose = new List<IDisposable>
                    {
                        _engine,
                        NotificationCenter,
                        LicenseManager,
                        DatabasesLandlord,
                        _env,
                        ContextPool,
                        ByteStringMemoryCache.Cleaner
                    };

                    var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {nameof(ServerStore)}.");

                    foreach (var disposable in toDispose)
                        exceptionAggregator.Execute(() =>
                        {
                            try
                            {
                                disposable?.Dispose();
                            }
                            catch (ObjectDisposedException)
                            {
                                //we are disposing, so don't care
                            }
                        });

                    exceptionAggregator.Execute(() => _shutdownNotification.Dispose());

                    exceptionAggregator.ThrowIfNeeded();
                }
                finally
                {
                    _disposed = true;
                }
            }


        }

        public void IdleOperations(object state)
        {
            try
            {
                foreach (var db in DatabasesLandlord.DatabasesCache)
                {
                    try
                    {
                        if (db.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = db.Value.Result;

                        if (DatabaseNeedsToRunIdleOperations(database))
                            database.RunIdleOperations();
                    }

                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    var maxTimeDatabaseCanBeIdle = Configuration.Databases.MaxIdleTime.AsTimeSpan;

                    var databasesToCleanup = DatabasesLandlord.LastRecentlyUsed
                        .Where(x => SystemTime.UtcNow - x.Value > maxTimeDatabaseCanBeIdle)
                        .Select(x => x.Key)
                        .ToArray();

                    foreach (var db in databasesToCleanup)
                    {
                        Task<DocumentDatabase> resourceTask;
                        if (DatabasesLandlord.DatabasesCache.TryGetValue(db, out resourceTask) &&
                            resourceTask != null &&
                            resourceTask.Status == TaskStatus.RanToCompletion &&
                            resourceTask.Result.PeriodicBackupRunner != null &&
                            resourceTask.Result.PeriodicBackupRunner.HasRunningBackups())
                        {
                            // there are running backups for this database
                            continue;
                        }

                        // intentionally inside the loop, so we get better concurrency overall
                        // since shutting down a database can take a while
                        DatabasesLandlord.UnloadDatabase(db, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle,
                            shouldSkip: database => database.Configuration.Core.RunInMemory);
                    }

                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error during idle operations for the server", e);
                }
            }
            finally
            {
                try
                {
                    _timer.Change(_frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private static bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database)
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            return ((now - maxLastWork).TotalMinutes > 5) || ((now - database.LastIdleTime).TotalMinutes > 10);
        }

        public void SecedeFromCluster()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                GenerateAuthenticationSignetureKeys(ctx);
            }
            Engine.Bootstrap(NodeHttpServerUrl, SignPublicKey, forNewCluster:true);
        }

        public Task<(long Etag, object Result)> WriteDatabaseRecordAsync(
            string databaseName, DatabaseRecord record, long? index,
            Dictionary<string, object> databaseValues = null, bool isRestore = false)
        {
            if (databaseValues == null)
                databaseValues = new Dictionary<string, object>();

            Debug.Assert(record.Topology != null);
            record.Topology.Stamp = new LeaderStamp
            {
                Term = _engine.CurrentTerm,
                LeadersTicks = _engine.CurrentLeader?.LeaderShipDuration ?? 0
            };

            var addDatabaseCommand = new AddDatabaseCommand
            {
                Name = databaseName,
                RaftCommandIndex = index,
                Record = record,
                DatabaseValues = databaseValues,
                IsRestore = isRestore
            };

            return SendToLeaderAsync(addDatabaseCommand);
        }

        public void EnsureNotPassive()
        {
            if (_engine.CurrentState == RachisConsensus.State.Passive)
            {
                _engine.Bootstrap(_ravenServer.ServerStore.NodeHttpServerUrl, SignPublicKey);
            }
        }


        public Task<(long Etag, object Result)> PutCommandAsync(CommandBase cmd)
        {
            return _engine.PutAsync(cmd);
        }

        public bool IsLeader()
        {
            return _engine.CurrentState == RachisConsensus.State.Leader;
        }

        public bool IsPassive()
        {
            return _engine.CurrentState == RachisConsensus.State.Passive;
        }

        public Task<(long Etag, object Result)> SendToLeaderAsync(CommandBase cmd)
        {
            return SendToLeaderAsyncInternal(cmd);
        }

        public DynamicJsonArray GetClusterErrors()
        {
            return _engine.GetClusterErrorsFromLeader();
        }

        public async Task<(long ClusterEtag, string ClusterId)> GenerateClusterIdentityAsync(string id, string databaseName)
        {
            var (etag, result) = await SendToLeaderAsync(new IncrementClusterIdentityCommand(databaseName)
            {
                Prefix = id.ToLower()
            });

            if (result == null)
            {

                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            return (etag, id + result);
        }

        public DatabaseRecord LoadDatabaseRecord(string databaseName, out long etag)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                return Cluster.ReadDatabase(context, databaseName, out etag);
            }
        }

        private async Task<(long Etag, object Result)> SendToLeaderAsyncInternal(CommandBase cmd)
        {
            //I think it is reasonable to expect timeout twice of error retry
            var timeoutTask = TimeoutManager.WaitFor(Engine.OperationTimeout, _shutdownNotification.Token);

            while (true)
            {
                ServerShutdown.ThrowIfCancellationRequested();

                if (_engine.CurrentState == RachisConsensus.State.Leader)
                {
                    return await _engine.PutAsync(cmd);
                }

                var logChange = _engine.WaitForHeartbeat();

                var cachedLeaderTag = _engine.LeaderTag; // not actually working
                try
                {
                    if (cachedLeaderTag == null)
                    {
                        await Task.WhenAny(logChange, timeoutTask);
                        if (logChange.IsCompleted == false)
                            ThrowTimeoutException();

                        continue;
                    }

                    return await SendToNodeAsync(cachedLeaderTag, cmd);
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Tried to send message to leader, retrying", ex);

                    if (_engine.LeaderTag == cachedLeaderTag)
                        throw; // if the leader changed, let's try again                    
                }

                await Task.WhenAny(logChange, timeoutTask);
                if (logChange.IsCompleted == false)
                    ThrowTimeoutException();
            }
        }

        private static void ThrowTimeoutException()
        {
            throw new TimeoutException("Could not send command to leader because there is no leader, and we timed out waiting for one");
        }

        private async Task<(long Etag, object Result)> SendToNodeAsync(string engineLeaderTag, CommandBase cmd)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var djv = cmd.ToJson(context);
                var cmdJson = context.ReadObject(djv, "raft/command");

                ClusterTopology clusterTopology;
                using (context.OpenReadTransaction())
                    clusterTopology = _engine.GetTopology(context);

                if (clusterTopology.Members.TryGetValue(engineLeaderTag, out string leaderUrl) == false)
                    throw new InvalidOperationException("Leader " + engineLeaderTag + " was not found in the topology members");

                var command = new PutRaftCommand(context, cmdJson);

                if (_clusterRequestExecutor == null
                    || _clusterRequestExecutor.Url.Equals(leaderUrl, StringComparison.OrdinalIgnoreCase) == false
                    || _clusterRequestExecutor.ApiKey?.Equals(clusterTopology.ApiKey) == false)
                {
                    _clusterRequestExecutor?.Dispose();
                    _clusterRequestExecutor = ClusterRequestExecutor.CreateForSingleNode(leaderUrl, clusterTopology.ApiKey);
                    _clusterRequestExecutor.ClusterToken = GetClusterTokenForNode(context);
                    _clusterRequestExecutor.DefaultTimeout = Engine.OperationTimeout;
                }

                await _clusterRequestExecutor.ExecuteAsync(command, context, ServerShutdown);

                return (command.Result.RaftCommandIndex, command.Result.Data);
            }
        }

        private class PutRaftCommand : RavenCommand<PutRaftCommandResult>
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _command;
            public override bool IsReadRequest => false;

            public PutRaftCommand(JsonOperationContext context, BlittableJsonReaderObject command)
            {
                _context = context;
                _command = command;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/rachis/send";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteObject(_command);
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationCluster.PutRaftCommandResult(response);
            }
        }

        public class PutRaftCommandResult
        {
            public long RaftCommandIndex { get; set; }

            public object Data { get; set; }
        }

        public Task WaitForTopology(Leader.TopologyModification state)
        {
            return _engine.WaitForTopology(state);
        }

        public Task WaitForState(RachisConsensus.State state)
        {
            return _engine.WaitForState(state);
        }

        public void ClusterAcceptNewConnection(Stream client)
        {
            _engine.AcceptNewConnection(client);
        }

        public async Task WaitForCommitIndexChange(RachisConsensus.CommitIndexModification modification, long value)
        {
            await _engine.WaitForCommitIndexChange(modification, value);
        }

        public string ClusterStatus()
        {
            return _engine.CurrentState + ", " + _engine.LastStateChangeReason;
        }

        private string _nodeHttpServerUrl;

        private string _nodeTcpServerUrl;

        public string NodeHttpServerUrl
        {
            get
            {
                if (_nodeHttpServerUrl != null)
                    return _nodeHttpServerUrl;

                Debug.Assert(_ravenServer.WebUrls != null && _ravenServer.WebUrls.Length > 0);
                return _nodeHttpServerUrl = Configuration.Core.GetNodeHttpServerUrl(
                    Configuration.Core.PublicServerUrl?.UriValue ?? _ravenServer.WebUrls[0]
                    );
            }
        }

        public string NodeTcpServerUrl
        {
            get
            {
                if (_nodeTcpServerUrl != null)
                    return _nodeTcpServerUrl;

                var webUrls = _ravenServer.WebUrls;
                Debug.Assert(webUrls != null && webUrls.Length > 0);
                var tcpStatusTask = _ravenServer.GetTcpServerStatusAsync();
                Debug.Assert(tcpStatusTask.IsCompleted);
                return _nodeTcpServerUrl = Configuration.Core.GetNodeTcpServerUrl(webUrls[0], tcpStatusTask.Result.Port);
            }
        }

        public DynamicJsonValue GetLogDetails(TransactionOperationContext context, int max = 100)
        {
            RachisConsensus.GetLastTruncated(context, out var index, out var term);
            var range = Engine.GetLogEntriesRange(context);
            var entries = new DynamicJsonArray();
            foreach (var entry in Engine.GetLogEntries(range.min, context, max))
            {
                entries.Add(entry.ToString());
            }

            var json = new DynamicJsonValue
            {
                [nameof(LogSummary.CommitIndex)] = Engine.GetLastCommitIndex(context),
                [nameof(LogSummary.LastTrancatedIndex)] = index,
                [nameof(LogSummary.LastTrancatedTerm)] = term,
                [nameof(LogSummary.FirstEntryIndex)] = range.min,
                [nameof(LogSummary.LastLogEntryIndex)] = range.max,
                [nameof(LogSummary.Entries)] = entries
            };
            return json;

        }
    }
}
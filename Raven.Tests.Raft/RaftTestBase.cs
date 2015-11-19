// -----------------------------------------------------------------------
//  <copyright file="RaftTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;

using Rachis.Transport;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Xunit;

namespace Raven.Tests.Raft
{
    public class RaftTestBase : RavenTestBase
    {
        private const int PortRangeStart = 9000;

        private static int numberOfPortRequests;
        private ILog _logger;

        public RaftTestBase()
        {
            _logger = LogManager.GetLogger("RaftTest." + GetType().FullName);
        }
        internal static int GetPort()
        {
            var portRequest = Interlocked.Increment(ref numberOfPortRequests);
            return PortRangeStart - (portRequest % 25);
        }

        public static IEnumerable<object[]> Nodes
        {
            get
            {
                return new[]
                {
                    new object[] { 1 },
                    new object[] { 3 },
                    new object[] { 5 }
                };
            }
        }

        public void WaitForDelete(IDatabaseCommands commands, string key, TimeSpan? timeout = null)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from the <system> database
                var doc = commands.Get(key);
                return doc == null;
            }, timeout ?? TimeSpan.FromMinutes(5));

            if (!done)
                throw new Exception("WaitForDelete failed");
        }

        public void WaitFor(IDatabaseCommands commands, Func<IDatabaseCommands, bool> action, TimeSpan? timeout = null)
        {
            var done = SpinWait.SpinUntil(() => action(commands), timeout ?? TimeSpan.FromMinutes(5));

            if (!done)
                throw new Exception("WaitFor failed");
        }

        public List<DocumentStore> CreateRaftCluster(int numberOfNodes, string activeBundles = null, Action<DocumentStore> configureStore = null, [CallerMemberName] string databaseName = null, bool inMemory = true)
        {
            var nodes = new List<RavenDbServer>();
            _logger.Info("Starting creating {0} Nodes", numberOfNodes);
            Parallel.For(0, numberOfNodes, x =>
            {
                var ravenDbServer = GetNewServer(GetPort(), activeBundles: activeBundles, databaseName: databaseName, runInMemory: inMemory);
                lock (nodes)
                {
                    nodes.Add(ravenDbServer);
                }
            });

            _logger.Info("{0} Nodes Created", numberOfNodes);
            var allNodesFinishedJoining = new ManualResetEventSlim();

            var random = new Random();
            var leader = nodes[random.Next(0, numberOfNodes - 1)];

            Console.WriteLine("Leader: " + leader.Options.ClusterManager.Value.Engine.Options.SelfConnection.Uri);

            leader.Options.ClusterManager.Value.InitializeTopology();
            //leader.Options.ClusterManager.Value.Engine.CurrentLeader = leader.Options.ClusterManager.Value.Engine.Name;
            _logger.Info("Leader Topology Initialized, leader is: {0}", leader.Options.ClusterManager.Value.Engine.CurrentLeader);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());
            _logger.Info("Finished Waiting for leader, leader is: {0}", leader.Options.ClusterManager.Value.Engine.CurrentLeader);
            leader.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
            {
                if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
                {
                    allNodesFinishedJoining.Set();
                }
            };

            for (var i = 0; i < numberOfNodes; i++)
            {
                var n = nodes[i];

                if (n == leader)
                    continue;

                Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(new NodeConnectionInfo
                                                                        {
                                                                            Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
                                                                            Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
                                                                        }).Wait(3000));
            }

            if (numberOfNodes == 1)
                allNodesFinishedJoining.Set();
            _logger.Info("Starting waiting to clusters being added");
            Assert.True(allNodesFinishedJoining.Wait(100000 * numberOfNodes), "Not all nodes become voters. " + leader.Options.ClusterManager.Value.Engine.CurrentTopology);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());
            _logger.Info("Waiting to clusters being added finished");
            WaitForClusterToBecomeNonStale(nodes);

            return nodes
                .Select(node => NewRemoteDocumentStore(ravenDbServer: node, activeBundles: activeBundles, configureStore: configureStore, databaseName: databaseName))
                .ToList();
        }

        public List<DocumentStore> ExtendRaftCluster(int numberOfExtraNodes, string activeBundles = null, Action<DocumentStore> configureStore = null, [CallerMemberName] string databaseName = null, bool inMemory = true)
        {
            var leader = servers.FirstOrDefault(server => server.Options.ClusterManager.Value.IsLeader());
            Assert.NotNull(leader);

            var nodes = Enumerable.Range(0, numberOfExtraNodes)
                .Select(x => GetNewServer(GetPort(), activeBundles: activeBundles, databaseName: databaseName, runInMemory:inMemory))
                .ToList();

            var allNodesFinishedJoining = new ManualResetEventSlim();
            leader.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
            {
                if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
                {
                    allNodesFinishedJoining.Set();
                }
            };

            for (var i = 0; i < numberOfExtraNodes; i++)
            {
                var n = nodes[i];

                if (n == leader)
                    continue;

                Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(new NodeConnectionInfo
                {
                    Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
                    Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
                }).Wait(10000));
                Assert.True(allNodesFinishedJoining.Wait(10000));
                allNodesFinishedJoining.Reset();
            }

            return nodes
                .Select(node => NewRemoteDocumentStore(ravenDbServer: node, activeBundles: activeBundles, configureStore: configureStore, databaseName: databaseName))
                .ToList();
        }

        public void RemoveFromCluster(RavenDbServer serverToRemove)
        {
            var leader = servers.FirstOrDefault(server => server.Options.ClusterManager.Value.IsLeader());
            if (leader == null)
                throw new InvalidOperationException("Leader is currently not present, thus can't remove node from cluster");
            if (leader == serverToRemove)
            {
                leader.Options.ClusterManager.Value.Engine.StepDownAsync().Wait();
            }
            else
            {
                leader.Options.ClusterManager.Value.Engine.RemoveFromClusterAsync(serverToRemove.Options.ClusterManager.Value.Engine.Options.SelfConnection).Wait(10000);
            }
        }

        private void WaitForClusterToBecomeNonStale(IReadOnlyCollection<RavenDbServer> nodes)
        {
            var numberOfNodes = nodes.Count;
            var result = SpinWait.SpinUntil(() => nodes.All(x => x.Options.ClusterManager.Value.Engine.CurrentTopology.AllVotingNodes.Count() == numberOfNodes), TimeSpan.FromSeconds(10));

            if (result == false)
                throw new InvalidOperationException("Cluster is stale.");
        }

        protected void WaitForClusterToBecomeNonStale(int numberOfNodes)
        {
            servers.ForEach(server => Assert.True(SpinWait.SpinUntil(() =>
            {
                var topology = server.Options.ClusterManager.Value.Engine.CurrentTopology;
                return topology.AllVotingNodes.Count() == numberOfNodes;
            }, TimeSpan.FromSeconds(15))));
        }

        protected void SetupClusterConfiguration(List<DocumentStore> clusterStores, bool enableReplication = true)
        {
            var clusterStore = clusterStores[0];
            var requestFactory = new HttpRavenRequestFactory();
            var replicationRequestUrl = string.Format("{0}/admin/cluster/commands/configuration", clusterStore.Url);
            var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Put, new RavenConnectionStringOptions
            {
                Url = clusterStore.Url
            });
            replicationRequest.Write(RavenJObject.FromObject(new ClusterConfiguration { EnableReplication = enableReplication }));
            replicationRequest.ExecuteRequest();

            clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));
            clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands =>
            {
                using (var request = commands.CreateRequest("/configuration/replication", HttpMethod.Get))
                {
                    var replicationDocumentJson = request.ReadResponseJson() as RavenJObject;
                    if (replicationDocumentJson == null) 
                        return false;

                    var replicationDocument = replicationDocumentJson.JsonDeserialization<ReplicationDocument>();
                    return replicationDocument.Destinations.Count == clusterStores.Count - 1;
                }
            }));
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class AddNodeToClusterTests : ReplicationTestBase
    {
        [Fact]
        public async Task FailOnAddingNonPassiveNode()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1);

            var url = raft2.WebUrl;
            await raft1.ServerStore.AddNodeToClusterAsync(url);
            Assert.True(await WaitForValueAsync(() => raft1.ServerStore.GetClusterErrors().Count > 0, true));
        }

        [Fact]
        public async Task PutDatabaseOnHealthyNodes()
        {
            var leader = await CreateRaftClusterAndGetLeader(5, leaderIndex: 0);
            var serverToDispose = Servers[1];
            await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);
            Assert.Equal(WaitForValue(() => leader.ServerStore.GetNodesStatuses().Count(n => n.Value.Connected), 3), 3);

            for (int i = 0; i < 5; i++)
            {
                var dbName = GetDatabaseName();
                var db = await CreateDatabaseInCluster(dbName, 4, leader.WebUrl);
                Assert.False(db.Servers.Contains(serverToDispose));
            }
        }

        [Fact]
        public async Task DisallowAddingNodeWithInvalidSourcePublicServerUrl()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1,customSettings: new Dictionary<string,string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)]  = "http://fake.url:8080"
            });
            var raft2 = await CreateRaftClusterAndGetLeader(1);

            var source = raft1.WebUrl;
            var dest = raft2.ServerStore.GetNodeHttpServerUrl();

            using (raft1.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(source, raft1.ServerStore.Server.Certificate.Certificate))
            {
                var nodeConnectionTest = new TestNodeConnectionCommand(dest, bidirectional: true);
                await requestExecutor.ExecuteAsync(nodeConnectionTest, context);
                var error = NodeConnectionTestResult.GetError(raft1.ServerStore.GetNodeHttpServerUrl(), dest);
                Assert.StartsWith(error, nodeConnectionTest.Result.Error);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    RequestUri = new Uri($"{source}/admin/cluster/node?url={dest}")
                };
                var response = await requestExecutor.HttpClient.SendAsync(request);
                Assert.False(response.IsSuccessStatusCode);
            }
        }

        [Fact]
        public async Task DisallowAddingNodeWithInvalidSourcePublicTcpServerUrl()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = "tcp://fake.url:54321"
            });
            var raft2 = await CreateRaftClusterAndGetLeader(1);

            var source = raft1.WebUrl;
            var dest = raft2.ServerStore.GetNodeHttpServerUrl();

            using (raft1.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(source, raft1.ServerStore.Server.Certificate.Certificate))
            {
                var nodeConnectionTest = new TestNodeConnectionCommand(dest, bidirectional: true);
                await requestExecutor.ExecuteAsync(nodeConnectionTest, context);
                var error = NodeConnectionTestResult.GetError(raft1.ServerStore.GetNodeHttpServerUrl(), dest);
                Assert.StartsWith(error, nodeConnectionTest.Result.Error);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    RequestUri = new Uri($"{source}/admin/cluster/node?url={dest}")
                };
                var response = await requestExecutor.HttpClient.SendAsync(request);
                Assert.False(response.IsSuccessStatusCode);
            }
        }

        [Fact]
        public async Task DisallowAddingNodeWithInvalidDestinationPublicServerUrl()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = "http://fake.url:54321"
            });

            var source = raft1.WebUrl;
            var dest = raft2.WebUrl;
            
            // here we pusblish a wrong PublicServerUrl, but connect to the ServerUrl, so the HTTP connection should be okay, but will when trying to the TCP connection.
            using (raft1.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(source, raft1.ServerStore.Server.Certificate.Certificate))
            {
                var nodeConnectionTest = new TestNodeConnectionCommand(dest, bidirectional: true);
                await requestExecutor.ExecuteAsync(nodeConnectionTest, context);
                var error = $"Was able to connect to url '{dest}', but exception was thrown while trying to connect to TCP port";
                Assert.StartsWith(error, nodeConnectionTest.Result.Error);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    RequestUri = new Uri($"{source}/admin/cluster/node?url={dest}")
                };
                var response = await requestExecutor.HttpClient.SendAsync(request);
                Assert.False(response.IsSuccessStatusCode);
            }
        }

        [Fact]
        public async Task DisallowAddingNodeWithInvalidDestinationPublicTcpServerUrl()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = "tcp://fake.url:54321"
            });

            var source = raft1.WebUrl;
            var dest = raft2.ServerStore.GetNodeHttpServerUrl();

            using (raft1.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(source, raft1.ServerStore.Server.Certificate.Certificate))
            {
                var nodeConnectionTest = new TestNodeConnectionCommand(dest, bidirectional: true);
                await requestExecutor.ExecuteAsync(nodeConnectionTest, context);
                var error = $"Was able to connect to url '{dest}', but exception was thrown while trying to connect to TCP port";
                Assert.StartsWith(error, nodeConnectionTest.Result.Error);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    RequestUri = new Uri($"{source}/admin/cluster/node?url={dest}")
                };
                var response = await requestExecutor.HttpClient.SendAsync(request);
                Assert.False(response.IsSuccessStatusCode);
            }
        }

        [Fact]
        public async Task AddDatabaseOnDisconnectedNode()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, leaderIndex: 0);
            await DisposeServerAndWaitForFinishOfDisposalAsync(Servers[1]);
            var db = GetDatabaseName();
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = db
            }.Initialize())
            {
                var hasDisconnected = await WaitForValueAsync(() => leader.ServerStore.GetNodesStatuses().Count(n => n.Value.Connected == false), 1) == 1;
                Assert.True(hasDisconnected);

                var record = new DatabaseRecord(db);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(record, clusterSize));
                var nodes = databaseResult.Topology.AllNodes.ToList();
                Assert.True(nodes.Contains("A"));
                Assert.True(nodes.Contains("B"));
                Assert.True(nodes.Contains("C"));
            }
        }

        [Fact]
        public async Task RemoveNodeWithDb()
        {
            var dbMain = GetDatabaseName();
            var dbWatcher = GetDatabaseName();

            var fromSeconds = Debugger.IsAttached ? TimeSpan.FromSeconds(15) : TimeSpan.FromSeconds(5);
            var leader = await CreateRaftClusterAndGetLeader(5);
            Assert.True(leader.ServerStore.LicenseManager.HasHighlyAvailableTasks());

            var db = await CreateDatabaseInCluster(dbMain, 5, leader.WebUrl);
            var watcherDb = await CreateDatabaseInCluster(dbWatcher, 1, leader.WebUrl);
            var serverNodes = db.Servers.Select(s => new ServerNode
            {
                ClusterTag = s.ServerStore.NodeTag,
                Database = dbMain,
                Url = s.WebUrl
            }).ToList();

            var conventions = new DocumentConventions
            {
                DisableTopologyUpdates = true
            };

            using (var watcherStore = new DocumentStore
            {
                Database = dbWatcher,
                Urls = new[] { watcherDb.Item2.Single().WebUrl },
                Conventions = conventions
            }.Initialize())
            using (var leaderStore = new DocumentStore
            {
                Database = dbMain,
                Urls = new[] { leader.WebUrl },
                Conventions = conventions
            }.Initialize())
            {
                var watcher = new ExternalReplication(dbWatcher, "Connection")
                {
                    MentorNode = Servers.First(s => s.ServerStore.NodeTag != watcherDb.Servers[0].ServerStore.NodeTag).ServerStore.NodeTag
                };

                Assert.True(watcher.MentorNode != watcherDb.Servers[0].ServerStore.NodeTag);

                var watcherRes = await AddWatcherToReplicationTopology((DocumentStore)leaderStore, watcher);
                var tasks = new List<Task>();
                foreach (var ravenServer in Servers)
                {
                    tasks.Add(ravenServer.ServerStore.Cluster.WaitForIndexNotification(watcherRes.RaftCommandIndex));
                }

                Assert.True(await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5)));

                var responsibleServer = Servers.Single(s => s.ServerStore.NodeTag == watcherRes.ResponsibleNode);
                using (var responsibleStore = new DocumentStore
                {
                    Database = dbMain,
                    Urls = new[] { responsibleServer.WebUrl },
                    Conventions = conventions
                }.Initialize())
                {
                    // check that replication works.
                    using (var session = leaderStore.OpenSession())
                    {
                        session.Advanced.WaitForReplicationAfterSaveChanges(timeout: fromSeconds, replicas: 4);
                        session.Store(new User
                        {
                            Name = "Karmel"
                        }, "users/1");
                        session.SaveChanges();
                    }

                    Assert.True(WaitForDocument<User>(watcherStore, "users/1", u => u.Name == "Karmel", 30_000));
                    
                    // remove the node from the cluster that is responsible for the external replication
                    await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(watcherRes.ResponsibleNode).WaitAsync(fromSeconds));
                    Assert.True(await responsibleServer.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitAsync(fromSeconds));

                    var dbInstance = await responsibleServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbMain);
                    await WaitForValueAsync(() => dbInstance.ReplicationLoader.OutgoingConnections.Count(), 0);

                    // replication from the removed node should be suspended
                    await Assert.ThrowsAsync<NodeIsPassiveException>(async () =>
                    {
                        using (var session = responsibleStore.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User
                            {
                                Name = "Karmel2"
                            }, "users/2");
                            await session.SaveChangesAsync();
                        }
                    });
                }
                
                var nodeInCluster = serverNodes.First(s => s.ClusterTag != responsibleServer.ServerStore.NodeTag);
                using (var nodeInClusterStore = new DocumentStore
                {
                    Database = dbMain,
                    Urls = new[] { nodeInCluster.Url },
                    Conventions = conventions
                }.Initialize())
                {
                    // the task should be reassinged within to another node
                    using (var session = nodeInClusterStore.OpenSession())
                    {
                        session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: 3);
                        session.Store(new User
                        {
                            Name = "Karmel3"
                        }, "users/3");
                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForDocument<User>(watcherStore, "users/3", u => u.Name == "Karmel3", 30_000));

                // rejoin the node
                var newLeader = await ActionWithLeader(l => l.ServerStore.AddNodeToClusterAsync(responsibleServer.WebUrl, watcherRes.ResponsibleNode));
                Assert.True(await responsibleServer.ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(fromSeconds));

                using (var newLeaderStore = new DocumentStore
                {
                    Database = dbMain,
                    Urls = new[] { newLeader.WebUrl },
                }.Initialize())
                using (var session = newLeaderStore.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: 3);
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel4"
                    }, "users/4");
                    await session.SaveChangesAsync();
                }
                
                Assert.True(WaitForDocument<User>(watcherStore, "users/4", u => u.Name == "Karmel4", 30_000), $"The watcher doesn't have the document");
            }
        }

        [Fact]
        public async Task RemoveRedundantPromotable()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            var db = GetDatabaseName();

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = db
            }.Initialize())
            {
                await cluster.Leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
                {
                    Record = new DatabaseRecord(db)
                    {
                        Topology = new DatabaseTopology
                        {
                            Members = new List<string> { "A" },
                            Promotables = new List<string> { "B", "C" },
                            ReplicationFactor = 2
                        }
                    },
                    Name = db
                });

                await WaitForAssertion(() =>
                {
                    using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, db);
                        Assert.Equal(0, record.DeletionInProgress?.Count ?? 0);

                        var topology = record.Topology;
                        Assert.Equal(2, topology.ReplicationFactor);
                        Assert.Equal(2, topology.Members.Count);
                        Assert.Equal(0, topology.Promotables.Count);
                        Assert.Equal(0, topology.Rehabs.Count);
                    }
                });
            }
        }

        [Fact]
        public async Task RemoveRedundantRehabs()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            var db = GetDatabaseName();

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = db
            }.Initialize())
            {
                await cluster.Leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
                {
                    Record = new DatabaseRecord(db)
                    {
                        Topology = new DatabaseTopology
                        {
                            Members = new List<string> { "A" },
                            Rehabs = new List<string> { "B", "C" },
                            ReplicationFactor = 2
                        }
                    },
                    Name = db
                });

                await WaitForAssertion(() =>
                {
                    using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, db);
                        Assert.Equal(0, record.DeletionInProgress?.Count ?? 0);

                        var topology = record.Topology;
                        Assert.Equal(2, topology.ReplicationFactor);
                        Assert.Equal(2, topology.Members.Count);
                        Assert.Equal(0, topology.Promotables.Count);
                        Assert.Equal(0, topology.Rehabs.Count);
                    }
                });
            }
        }

        [Fact]
        public async Task RemoveRedundantNodes()
        {
            var clusterSize = 5;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            var db = GetDatabaseName();

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = db
            }.Initialize())
            {
                await cluster.Leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
                {
                    Record = new DatabaseRecord(db)
                    {
                        Topology = new DatabaseTopology
                        {
                            Members = new List<string> { "A" },
                            Rehabs = new List<string> { "B", "C" },
                            Promotables = new List<string> { "D", "E" },
                            ReplicationFactor = 3
                        }
                    },
                    Name = db
                });

                await WaitForAssertion(() =>
                {
                    using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, db);
                        Assert.Equal(0, record.DeletionInProgress?.Count ?? 0);

                        var topology = record.Topology;
                        Assert.Equal(3, topology.ReplicationFactor);
                        Assert.Equal(3, topology.Members.Count);
                        Assert.Equal(0, topology.Promotables.Count);
                        Assert.Equal(0, topology.Rehabs.Count);
                    }
                });
            }
        }

        [Fact]
        public async Task FailOnAddingNodeWhenLeaderHasPortZero()
        {
            var leader = await CreateRaftClusterAndGetLeader(1);
            leader.ServerStore.ValidateFixedPort = true;

            var server2 = GetNewServer();
            var server2Url = server2.ServerStore.GetNodeHttpServerUrl();
            Servers.Add(server2);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => 
                await leader.ServerStore.AddNodeToClusterAsync(server2Url));

            Assert.Contains("Adding nodes to cluster is forbidden when the leader " +
                            "has port '0' in 'Configuration.Core.ServerUrls' setting", ex.Message);

        }

        [Fact]
        public async Task FailOnAddingNodeThatHasPortZero()
        {
            var leader = await CreateRaftClusterAndGetLeader(1);
            leader.ServerStore.Configuration.Core.ServerUrls = new[] { leader.WebUrl };
            leader.ServerStore.ValidateFixedPort = true;

            var server2 = GetNewServer();
            var server2Url = server2.ServerStore.GetNodeHttpServerUrl();
            Servers.Add(server2);

            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null))
            using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(server2Url), ctx));

                Assert.Contains($"Node '{server2Url}' has port '0' in 'Configuration.Core.ServerUrls' setting. " +
                                "Adding a node with non fixed port is forbidden. Define a fixed port for the node to enable cluster creation.", ex.Message);
            }
        }

        [Fact]
        public async Task CanSnapshotCompareExchangeTombstones()
        {
            var leader = await CreateRaftClusterAndGetLeader(1);


            using (var store = GetDocumentStore(options:new Options
            {
                Server = leader
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("foo", "bar");
                    await session.SaveChangesAsync();

                    var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>("foo");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(result);
                    await session.SaveChangesAsync();
                }

                var server2 = GetNewServer();
                var server2Url = server2.ServerStore.GetNodeHttpServerUrl();
                Servers.Add(server2);

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null))
                using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(server2Url, watcher: true), ctx);

                    var addDatabaseNode = new AddDatabaseNodeOperation(store.Database);
                    await store.Maintenance.Server.SendAsync(addDatabaseNode);
                }

                using (server2.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.True(server2.ServerStore.Cluster.HasCompareExchangeTombstones(ctx, store.Database));
                }
            }
        }

        [Fact]
        public async Task ResetServerShouldPreserveTopology()
        {
            var cluster = await CreateRaftCluster(3, shouldRunInMemory: false);
            var followers = cluster.Nodes.Where(x => x != cluster.Leader);
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            foreach (var follower in followers)
            {
                
                while (cluster.Leader.ServerStore.Engine.CurrentLeader.TryModifyTopology(follower.ServerStore.NodeTag, follower.ServerStore.Engine.Url, Leader.TopologyModification.NonVoter, out var task) == false)
                {
                    await task;
                }
            }

            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Leader);
            cluster.Leader = GetNewServer(new ServerCreationOptions {DeletePrevious = false, RunInMemory = false, PartialPath = result.DataDir, CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url
            }});

            var topology = cluster.Leader.ServerStore.GetClusterTopology();
            Assert.Equal(3, topology.AllNodes.Count);
        }
        private async Task WaitForAssertion(Action action)
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    action();
                    return;
                }
                catch
                {
                    if (sp.ElapsedMilliseconds > 10_000)
                        throw;

                    await Task.Delay(100);
                }
            }
        }
    }
}

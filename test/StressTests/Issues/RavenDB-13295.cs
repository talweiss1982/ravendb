using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Sparrow.Server;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents;
using System.Threading;

namespace StressTests.Issues
{
    public class RavenDB_13295 : Tests.Infrastructure.ClusterTestBase
    {
        public string DatabaseName { get; private set; } = "CanPutAndReadClusterTx";

        [Fact]
        public async Task CanPutAndReadClusterTx()
        {
            var leader = await CreateRaftClusterAndGetLeader(3,leaderIndex:0, shouldRunInMemory:false);
            var amre = new AsyncManualResetEvent();
            using (var leaderStore = GetDocumentStore(new Options {ReplicationFactor = 3,Server = leader }))
            {
                var cts = new CancellationTokenSource();
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                BombardClusterTx(Servers[0], amre, leaderStore.Database, cts.Token);
                BombardClusterTx(Servers[1], amre, leaderStore.Database, cts.Token);
                BombardClusterTx(Servers[2], amre, leaderStore.Database, cts.Token);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                await amre.WaitAsync();
                cts.Cancel();
                Console.Read();
            }            
        }

        private async Task BombardClusterTx(RavenServer server, AsyncManualResetEvent amre, string databaseName, CancellationToken token)
        {
            using (var store = new DocumentStore()
            {
                Urls = new[] { server.WebUrl },
                Database = databaseName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                Console.WriteLine(server.WebUrl);
                var tag = server.ServerStore.Engine.Tag;
                int etag = 0;
                //var prefix = 
                while (true)
                {
                    if (token.IsCancellationRequested)
                        return;
                    try
                    {
                        var key = $"User/{etag++}-{tag}";
                        using (var session = store.OpenAsyncSession(new SessionOptions
                        {
                            TransactionMode = TransactionMode.ClusterWide,
                        }))
                        {

                            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, 1);
                            await session.StoreAsync(new User { Name = key });
                            await session.SaveChangesAsync();
                        }
                        using (var session = store.OpenAsyncSession(new SessionOptions
                        {
                            TransactionMode = TransactionMode.ClusterWide
                        }))
                        {
                            var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(key);
                            if (value == null)
                            {
                                var color = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine("Bazinga!");
                                Console.ForegroundColor = color;
                                amre.Set();
                            }

                        }
                    } catch(Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }
        }
    }
}

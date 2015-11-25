using System;
using System.IO;
using System.Linq;
using System.Threading;
using NLog;
using NLog.Config;
using NLog.Targets;
using Rachis.Tests;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Core;
using Raven.Tests.Core.Replication;
using Raven.Tests.Raft;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var i=2;
          /*  if (args.Length >= 1)
            {
                int.TryParse(args[0], out i);
            }*/
            Console.WriteLine(i);
            using (var x = new Test())
            {
                x.DeleteShouldBePropagated(i);
            }
            Console.WriteLine("Finishd Running");
/*            new GrishaDaKing().DisasterCluster();*/
        }

        public class GrishaDaKing: RavenReplicationCoreTest
        {
            
            public void DisasterCluster()
            {
                this.SetFixture(new TestServerFixture()
                {
                });
                var stores = Enumerable.Range(0, 8).Select(x => GetDocumentStore()).ToList();
                stores.ForEach(x => x.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(x.DefaultDatabase));

                foreach (var documentStore in stores)
                {
                    SetupReplication(documentStore, stores.Where(x=>x!=documentStore).ToArray());
                }
                for (var i=0;  i< stores.Count();i++)
                {
                    stores[i].DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
                }

                WaitForUserToContinueTheTest(stores.First());
                

            }
            
        }
        public class Test : RaftTestBase
        {

            public void DeleteShouldBePropagated(int numberOfNodes)
            {
                var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader, inMemory:true);
                Console.WriteLine("Cluster Created");
                SetupClusterConfiguration(clusterStores);
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

#pragma warning disable 618
                    store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
#pragma warning restore 618
                }
                
                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.Get("keys/" + i) != null));
#pragma warning restore 618
                }
                WaitForUserToContinueTheTest(url: clusterStores[0].Url);
                //WaitForUserToContinueTheTest(url: stores.First().Url);
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

#pragma warning disable 618
                    store.DatabaseCommands.Delete("keys/" + i, null);
#pragma warning restore 618
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.Get("keys/" + i) == null));
#pragma warning restore 618
                }
                //WaitForUserToContinueTheTest(url: stores.First().Url);
            }
        }
    }
}

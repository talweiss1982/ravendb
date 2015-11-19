using System;
using System.IO;
using NLog;
using NLog.Config;
using NLog.Targets;
using Rachis.Tests;
using Raven.Abstractions.Cluster;
using Raven.Json.Linq;
using Raven.Tests.Raft;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var i=4;
            if (args.Length >= 1)
            {
                int.TryParse(args[0], out i);
            }
            Console.WriteLine(i);
            using (var x = new Test())
            {
                x.DeleteShouldBePropagated(i);
            }
            Console.WriteLine("Finishd Running");
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
                    store.DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetAttachment("keys/" + i) != null));
#pragma warning restore 618
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

#pragma warning disable 618
                    store.DatabaseCommands.DeleteAttachment("keys/" + i, null);
#pragma warning restore 618
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetAttachment("keys/" + i) == null));
#pragma warning restore 618
                }
            }
        }
    }
}

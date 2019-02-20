using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests : ReplicationTestBase
    {
        [Fact64Bit]
        public void ExternalReplicationShouldWorkWithSmallTimeoutStress()
        {
            for (int i = 0; i < 10_000; i++)
            {
                Console.WriteLine("iteration "+i);
                Parallel.For(0, 5, RavenTestHelper.DefaultParallelOptions, _ =>
                {
                    using (var test = new ExternalReplicationTests())
                    {
                        test.ExternalReplicationShouldWorkWithSmallTimeoutStress().Wait();
                    }
                });
            }
        }
    }
}

﻿using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace SlowTests.Server.Documents.Indexing
{
    public class LiveIndexingPerformanceCollectorTests : RavenTestBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public override string IndexName
            {
                get { return "Users/ByName"; }
            }

            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        [Fact]
        public async Task CanObtainRecentIndexingPerformance()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                var index = database.IndexStore.GetIndex("Users/ByName");

                var collector = new LiveIndexingPerformanceCollector(database, database.DatabaseShutdown, new[] {index});

                var tuple = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(1));
                Assert.True(tuple.Item1);
                var stats = tuple.Item2;

                Assert.Equal(1, stats.Count);
                var usersStats = stats[0];
                Assert.Equal("Users/ByName", usersStats.IndexName);

                Assert.Equal(2, usersStats.Performance.Select(x => x.InputCount).Sum());
            }
        }

        [Fact]
        public async Task CanObtainLiveIndexingPerformanceStats()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                var database = await GetDocumentDatabaseInstanceFor(store);

                new UsersByName().Execute(store);
                
                var index = database.IndexStore.GetIndex("Users/ByName");

                var collector = new LiveIndexingPerformanceCollector(database, database.DatabaseShutdown, new[] { index });

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var dequeueCount = 0;

                var tuple = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(5));
                Assert.True(tuple.Item1);
                dequeueCount++;
                var usersStats = tuple.Item2[0];

                while (true)
                {
                    if (usersStats.Performance.Select(x => x.InputCount).Sum() == 2)
                        break;

                    tuple = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(10));
                    dequeueCount++;

                    if (tuple.Item1 == false)
                        break;
                    Assert.Equal(1, tuple.Item2.Count);
                    usersStats = tuple.Item2[0];
                }
                Assert.Equal("Users/ByName", usersStats.IndexName);

                Assert.True(usersStats.Performance.Select(x => x.InputCount).Sum() == 2, $"Can\'t find indexing performance stats, after {dequeueCount} tries");
            }
        }
    }
}
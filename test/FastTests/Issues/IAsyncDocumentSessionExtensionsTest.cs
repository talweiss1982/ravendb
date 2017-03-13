﻿using System.Threading.Tasks;
using Xunit;

namespace FastTests.Issues
{
    public class IAsyncDocumentSessionExtensionsTest : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        [Fact]
        public async Task CanStreamStartWithAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var session = store.OpenAsyncSession())
                {
                    using (var reader = await session.Advanced.StreamAsync<User>("users/"))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }
        [Fact]
        public void CanStreamStartWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var session = store.OpenSession())
                {
                    using (var reader = session.Advanced.Stream<User>("users/"))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }
    }
}
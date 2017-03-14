//-----------------------------------------------------------------------
// <copyright file="Versioning.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Exceptions.Versioning;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Versioning
{
    public class Versioning : RavenTestBase
    {
        [Fact]
        public async Task CanGetAllRevisionsFor()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task CanCheckIfDocumentIsVersioned()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal("Versioned", metadata["@flags"]);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey()
        {

            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    Assert.Equal(0, companiesRevisions.Count);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey_WithVersioningDisabled()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var exception = await Assert.ThrowsAsync<VersioningDisabledException>(async () => await session.Advanced.GetRevisionsForAsync<Company>("companies/1"));
                    Assert.Contains("Versioning is disabled", exception.Message);
                }
            }
        }

        [Fact]
        public async Task CanExcludeEntitiesFromVersioning()
        {
            var user = new User { Name = "User Name" };
            var comment = new Comment { Name = "foo" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.StoreAsync(comment);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Empty(await session.Advanced.GetRevisionsForAsync<Comment>(comment.Id));
                    var users = await session.Advanced.GetRevisionsForAsync<User>(user.Id);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task ServerSaveBundlesAfterRestart()
        {
            var path = NewDataPath();
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore(path: path))
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }
            }

            using (var store = GetDocumentStore(path: path))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task WillCreateRevisionIfExplicitlyRequested()
        {
            var product = new Product { Description = "A fine document db", Quantity = 5 };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 2";
                    await session.StoreAsync(product);
                    session.Advanced.ExplicitlyVersion(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 3";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var products = await session.Advanced.GetRevisionsForAsync<Product>(product.Id);
                    Assert.Equal("desc 2", products.Single().Description);
                }
            }
        }

        [Fact]
        public async Task WillDeleteOldRevisions()
        {
            var company = new Company { Name = "Company #1" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    for (var i = 0; i < 10; i++)
                    {
                        company.Name = "Company #2: " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
                    Assert.Equal(5, revisions.Count);
                    Assert.Equal("Company #2: 5", revisions[0].Name);
                    Assert.Equal("Company #2: 9", revisions[4].Name);
                }
            }
        }

        [Fact]
        public async Task WillDeleteRevisionsIfDeleted_OnlyIfPurgeOnDeleteIsTrue()
        {
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Hibernaitng Rhinos " };
                    var user = new User { Name = "Fitzchak " };
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>("companies/1");
                        var user = await session.LoadAsync<User>("users/1");
                        company.Name += i;
                        user.Name += i;
                        await session.StoreAsync(company);
                        await session.StoreAsync(user);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(company);
                    Assert.NotNull(user);
                    session.Delete(company);
                    session.Delete(user);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    var users = await session.Advanced.GetRevisionsForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Empty(users);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "New Company" }, "companies/1");
                    await session.StoreAsync(new User { Name = "New User" }, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    var users = await session.Advanced.GetRevisionsForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Equal("New Company", companies.Last().Name);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        private class Comment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Description { get; set; }
            public int Quantity { get; set; }
        }
    }
}
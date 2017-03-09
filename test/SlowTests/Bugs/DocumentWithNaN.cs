// -----------------------------------------------------------------------
//  <copyright file="DocumentWithNaN.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class Number
    {
        public float FNumber = Single.NaN;
    }
    public class DocumentWithNaN : RavenTestBase
    {
        [Fact]
        public async Task CanSaveUsingLegacyMode()
        {
            using(var store = GetDocumentStore())
            using(var session = store.OpenSession())
            {
                var httpClient = new HttpClient
                {
                    BaseAddress = new Uri(store.Url)
                };

                var httpResponseMessage = await httpClient.PutAsync($"/databases/{store.DefaultDatabase}/docs?id=items/1",
                    new StringContent("{'item': NaN}"));
                Assert.True(httpResponseMessage.IsSuccessStatusCode);

                session.Store(new Number());
                session.SaveChanges();
                var num = session.Load<Number>("Numbers/1");
                Assert.Equal(float.NaN, num.FNumber);
            }
        }
         
    }
}

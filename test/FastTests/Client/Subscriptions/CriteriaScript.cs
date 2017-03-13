﻿using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class CriteriaScript : SubscriptionTestBase
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task BasicCriteriaTest(bool useSsl)
        {
            if (useSsl)
            {
                DoNotReuseServer(new global::Sparrow.Collections.LockFree.ConcurrentDictionary<string, string> { { "Raven/UseSsl", "true" } });
            }
            using (var store = GetDocumentStore())
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria("Things")
                {
                    FilterJavaScript = " return this.Name == 'ThingNo3'",
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                using (var subscription = subscriptionManager.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var list = new BlockingCollection<Thing>();
                    subscription.Subscribe(x =>
                    {
                        list.Add(x);
                    });
                    await subscription.StartAsync();

                    Thing thing;
                    Assert.True(list.TryTake(out thing, 5000));
                    Assert.Equal("ThingNo3", thing.Name);
                    Assert.False(list.TryTake(out thing, 50));
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CriteriaScriptWithTransformation(bool useSsl)
        {
            if (useSsl)
            {
                DoNotReuseServer(new global::Sparrow.Collections.LockFree.ConcurrentDictionary<string, string> { { "Raven/UseSsl", "true" } });
            }
            using (var store = GetDocumentStore())
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 6);

                var subscriptionCriteria = new SubscriptionCriteria("Things")
                {
                    FilterJavaScript =
                    @"var namSuffix = parseInt(this.Name.replace('ThingNo', ''));  
                    if (namSuffix <= 2){
                        return false;
                    }
                    else if (namSuffix == 3){
                        return null;
                    }
                    else if (namSuffix == 4){
                    return this;
                    }
                    return {Name: 'foo', OtherDoc:LoadDocument('things/6')}",
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                using (var subscription = subscriptionManager.Open<BlittableJsonReaderObject>(new SubscriptionConnectionOptions(subsId)))
                {
                    JsonOperationContext context;
                    using (store.GetRequestExecuter().ContextPool.AllocateOperationContext(out context))
                    {
                        var list = new BlockingCollection<BlittableJsonReaderObject>();
                        subscription.Subscribe(x =>
                        {
                            list.Add(context.ReadObject(x, "test"));
                        });
                        await subscription.StartAsync();

                        BlittableJsonReaderObject thing;

                        Assert.True(list.TryTake(out thing, 5000)); // change this back to 5000
                        dynamic dynamicThing = new DynamicBlittableJson(thing);
                        Assert.Equal("ThingNo4", dynamicThing.Name);


                        Assert.True(list.TryTake(out thing, 5000)); // change this back to 5000
                        dynamicThing = new DynamicBlittableJson(thing);
                        Assert.Equal("foo", dynamicThing.Name);
                        Assert.Equal("ThingNo4", dynamicThing.OtherDoc.Name);


                      
                        
                        Assert.False(list.TryTake(out thing, 50));

                    }
                }
            }
        }
    }
}

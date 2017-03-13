﻿using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Xunit;
using Sparrow;

namespace FastTests.Client.Subscriptions
{
    public class Subscriptions : SubscriptionTestBase
    {
        [Fact]
        public async Task CreateSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCriteria = new SubscriptionCriteria("People");
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria);

                var subscriptionsConfig = await store.AsyncSubscriptions.GetSubscriptionsAsync(0, 10);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCriteria.Collection, subscriptionsConfig[0].Criteria.Collection);
                Assert.Equal(subscriptionCriteria.FilterJavaScript, subscriptionsConfig[0].Criteria.FilterJavaScript);
                Assert.Equal(0, subscriptionsConfig[0].AckEtag);
                Assert.Equal(subsId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria("Things");
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (var subscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var list = new BlockingCollection<Thing>();
                    subscription.Subscribe<Thing>(x =>
                    {
                        list.Add(x);
                    });
                    await subscription.StartAsync();

                    Thing thing;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(list.TryTake(out thing, 1000));
                    }
                    Assert.False(list.TryTake(out thing, 50));
                }
            }
        }

        [Fact]
        public async Task SubscriptionStrategyConnectIfFree()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria("Things");
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetryMilliseconds = 20000
                    }))
                {

                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    acceptedSubscription.Subscribe(x =>
                    {
                        acceptedSusbscriptionList.Add(x);
                    });
                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 1000));
                    }

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing, 50));

                    // open second subscription
                    using (
                        var rejectedSusbscription =
                            store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                            {
                                Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                                TimeToWaitBeforeConnectionRetryMilliseconds = 2000
                            }))
                    {

                        rejectedSusbscription.Subscribe(thing1 => { });

                        // sometime not throwing (on linux) when written like this:
                        // await Assert.ThrowsAsync<SubscriptionInUseException>(async () => await rejectedSusbscription.StartAsync());
                        // so we put this in a try block
                        try
                        {
                            await rejectedSusbscription.StartAsync();
                            Assert.False(true); // we didn't throw - so test failed
                        }
                        catch (SubscriptionInUseException)
                        {

                        }

                    }
                }
            }
        }

        [Fact(Skip = "RavenDB-5986")]
        public async Task SubscriptionWaitStrategy()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria("Things");
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {

                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var waitingSubscriptionList = new BlockingCollection<Thing>();

                    var ackSentAmre = new AsyncManualResetEvent();
                    acceptedSubscription.AfterAcknowledgment += () => ackSentAmre.SetByAsyncCompletion();

                    acceptedSubscription.Subscribe(x =>
                    {
                        acceptedSusbscriptionList.Add(x);
                        Thread.Sleep(20);
                    });

                    await acceptedSubscription.StartAsync();

                    // wait until we know that connection was established

                    Thing thing;
                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 50000));
                    }

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing, 50));

                    // open second subscription
                    using (
                        var waitingSubscription =
                            store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                            {
                                Strategy = SubscriptionOpeningStrategy.WaitForFree,
                                TimeToWaitBeforeConnectionRetryMilliseconds = 250
                            }))
                    {

                        waitingSubscription.Subscribe(x =>
                        {
                            waitingSubscriptionList.Add(x);
                        });
                        var taskStarted = waitingSubscription.StartAsync();
                        var completed = await Task.WhenAny(taskStarted, Task.Delay(60000));


                        Assert.False(completed == taskStarted);

                        Assert.True(await ackSentAmre.WaitAsync(TimeSpan.FromSeconds(50)));

                        acceptedSubscription.Dispose();

                        await CreateDocuments(store, 5);

                        // wait until we know that connection was established
                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(waitingSubscriptionList.TryTake(out thing, 1000));
                        }

                        Assert.False(waitingSubscriptionList.TryTake(out thing, 50));
                    }
                }
            }
        }

        [Fact]
        public async Task SubscriptionSimpleTakeOverStrategy()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria("Things");
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);

                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var takingOverSubscriptionList = new BlockingCollection<Thing>();
                    long counter = 0;
                    acceptedSubscription.Subscribe(x =>
                    {
                        Interlocked.Increment(ref counter);
                        acceptedSusbscriptionList.Add(x);
                    });

                    var batchProccessedByFirstSubscription = new AsyncManualResetEvent();

                    acceptedSubscription.AfterAcknowledgment +=
                        () =>
                        {
                            if (Interlocked.Read(ref counter) == 5)
                                batchProccessedByFirstSubscription.SetByAsyncCompletion();
                        };

                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 5000), "no doc");
                    }

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(15)), "no ack");

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing));

                    // open second subscription
                    using (var takingOverSubscription = store.AsyncSubscriptions.Open<Thing>(
                        new SubscriptionConnectionOptions(subsId)
                    {
                        
                        Strategy = SubscriptionOpeningStrategy.TakeOver
                    }))
                    {
                        takingOverSubscription.Subscribe(x => takingOverSubscriptionList.Add(x));
                        await takingOverSubscription.StartAsync();

                        await CreateDocuments(store, 5);

                        // wait until we know that connection was established
                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(takingOverSubscriptionList.TryTake(out thing, 5000), "no doc takeover");
                        }
                        Assert.False(takingOverSubscriptionList.TryTake(out thing));
                    }
                }
            }
        }
    }
}
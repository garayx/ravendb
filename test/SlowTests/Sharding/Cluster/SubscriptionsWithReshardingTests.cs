using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NuGet.Packaging;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Subscriptions;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class SubscriptionsWithReshardingTests : ClusterTestBase
    {
        public SubscriptionsWithReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterResharding()
        {
            using var store = Sharding.GetDocumentStore();
            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentOnce()
        {
            using var store = Sharding.GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1-A");
                session.SaveChanges();
            }

            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
            {
                MaxDocsPerBatch = 5,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            }))
            {
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }
                    }
                });


                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce6()
        {
            DoNotReuseServer();
            Console.WriteLine($"{this.Server.WebUrl}");
            using var store = Sharding.GetDocumentStore(
                new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration()
                        {
                            Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } }
                        };
                    }
                }
            );
            Server.ServerStore.Sharding.ManualMigration = true;
            var id = "users/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new User { }, id);
                session.SaveChanges();
            }

            var lastId = string.Empty;
            var lastCV = string.Empty;
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 8; i++)
                {
                    lastId = $"num-{i}${id}";
                    session.Store(new User { }, lastId);
                }

                session.SaveChanges();

                lastCV= session.Advanced.GetChangeVectorFor(lastId);
            }

            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var sub = await store.Subscriptions.CreateAsync<User>();
            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
            {
                MaxDocsPerBatch = 1,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            });

            var t11 = subscription.Run(batch =>
            {
                foreach (var item in batch.Items)
                {
                    if (users.Add(item.Id) == false)
                    {

                    }
                }
            });
            var state = store.Subscriptions.GetSubscriptionState(sub);
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(record.Sharding, id);
            var curShard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, curShard);

            var sourceName = ShardHelper.ToShardName(store.Database, curShard);
            var destName = ShardHelper.ToShardName(store.Database, toShard);
            var srcDb = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(sourceName);
            var destDb = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(destName);
            var res = WaitForValue(() =>
            {
                string cv1 = null;
                using (srcDb.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var state = store.Subscriptions.GetSubscriptionState(sub);
                    var s = srcDb.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                    var c = s?.GetConnections().FirstOrDefault();
                    var subsState = c?.SubscriptionState;
                    subsState?.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(sourceName, out cv1);
                    return cv1;
                }
            }, lastCV, interval: 333);

            Assert.Equal(lastCV, res);


            var srcTestingStuff = srcDb.ForTestingPurposesOnly();
            var srcTransientError = srcTestingStuff.CallAfterRegisterSubscriptionConnection(_ => throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });


            using (srcDb.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var s = srcDb.SubscriptionStorage.DropSubscriptionConnections(state.SubscriptionId,
                    new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });
                Assert.True(s);
            }

            using (var session = store.OpenSession(sourceName))
            {
                for (int i = 0; i < 7; i++)
                {
                    lastId = $"num-{i}${id}";
                    session.Store(new User { }, lastId);
                    session.SaveChanges();
                }


            }


            /*var cmd = new StartBucketMigrationCommand(bucket, curShard, toShard, store.Database, RaftIdGenerator.NewId());
            var result = await Server.ServerStore.SendToLeaderAsync(cmd);*/


            var testingStuff = destDb.ForTestingPurposesOnly();
            var transientError = testingStuff.CallAfterRegisterSubscriptionConnection(_ => throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });





            await Sharding.Resharding.StartManualShardMigrationForId(store, lastId, Server, bucket, curShard, toShard);
            //Thread.Sleep(10_000);
            srcDb.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

            using (var session = store.OpenSession(sourceName))
            {
                for (int i = 0; i < 15; i++)
                {
                    lastId = $"num-{i}${id}";
                    session.Store(new User { }, lastId);
                    session.SaveChanges();
                }

          
            }
            WaitForUserToContinueTheTest(store);
        }














        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce5()
        {
           // DoNotReuseServer();
            using var store = Sharding.GetDocumentStore(
                new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration() { Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } } };
                    }
                }
            );
          //  Server.ServerStore.Sharding.ManualMigration = true;
          //  var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var id = "users/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                }, id);
                session.SaveChanges();
            }

            var lastId = string.Empty;
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    lastId = $"num-{i}${id}";
                    session.Store(new User { }, lastId);
                }
                session.SaveChanges();
            }

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);


            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(record.Sharding, id);
            var curShard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, curShard);

            var sourceName = ShardHelper.ToShardName(store.Database, curShard);
            var destName = ShardHelper.ToShardName(store.Database, toShard);
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(sourceName);

            await db.DocumentsMigrator.ExecuteMoveDocumentsAsync();


            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
            {
                MaxDocsPerBatch = 1,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            });

            db.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

            using (var session = store.OpenSession(sourceName))
            {
                session.Store(new User { Name = "EGR" }, $"num-322$users/1-A");
                session.SaveChanges();
            }

            Thread.Sleep(Int32.MaxValue);













            /*using (Sharding.Resharding.ForTestingPurposesOnly().CallBetweenSourceAndDestinationMigrationCompletion(() =>
            {
                // ReSharper disable once AccessToDisposedClosure
                /*using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, $"num-322$users/1-A");
                    session.SaveChanges();
                }#1#
            }))
            {
                await Sharding.Resharding.StartManualShardMigrationForId(store, lastId, Server);
                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
                {
                    MaxDocsPerBatch = 1,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                });



            }*/



            Thread.Sleep(int.MaxValue);


        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce4()
        {
            DoNotReuseServer();
            Console.WriteLine($"{this.Server.WebUrl}");
            using var store = Sharding.GetDocumentStore(
                new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration() { Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } } };
                    }
                }
            );
            Server.ServerStore.Sharding.ManualMigration = true;
            //var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var id = "users/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                }, id);
                session.SaveChanges();
            }

            var lastId = string.Empty;
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 12; i++)
                {
                    lastId = $"num-{i}${id}";
                    session.Store(new User { }, lastId);
                }
                session.SaveChanges();
            }

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);


            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
            {
                MaxDocsPerBatch = 1,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            });

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(record.Sharding, id);
            var curShard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, curShard);

            var sourceName = ShardHelper.ToShardName(store.Database, curShard);
            var destName = ShardHelper.ToShardName(store.Database, toShard);
            var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(sourceName);
            var db2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(destName);
        




            using (var session = store.OpenAsyncSession(sourceName))
            {
                var user = await session.LoadAsync<dynamic>(id);
                Assert.NotNull(user);
            }

            /*var cmd = new StartBucketMigrationCommand(bucket, curShard, toShard, store.Database, RaftIdGenerator.NewId());
            var result = await Server.ServerStore.SendToLeaderAsync(cmd);*/

            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());
            var migrationIndex = result.Index;

            var exists = WaitForDocument<dynamic>(store, id, predicate: null, database: destName);
            Assert.True(exists);


            var t11 = subscription.Run(batch =>
            {
                foreach (var item in batch.Items)
                {
                    if (users.Add(item.Id) == false)
                    {

                    }
                }
            });


            var shards1 = await Sharding.GetShardsDocumentDatabaseInstancesFor(store).ToListAsync();
            var state = await store.Subscriptions.GetSubscriptionStateAsync(sub);

            string cv1;


            var res = WaitForValue(() =>
                    {
                        string cv2;
                        foreach (var db in shards1)
                        {
                            using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {

                                var s = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                                var c = s?.GetConnections().FirstOrDefault();
                                var subsState = c?.SubscriptionState;
                                subsState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(sourceName, out cv1);
                                subsState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(destName, out  cv2);
                                if (string.IsNullOrEmpty(cv1) == false && string.IsNullOrEmpty(cv2) == false)
                                {
                                    return true;
                                }
                            }
                        }
            
                        return false;
                    }, true, interval: 333);

                    Assert.True(res);


                    // 
            // var ts = db2.ShardedDocumentsStorage.ForTestingPurposesOnly();
            var testingStuff = db2.ForTestingPurposesOnly();
            var transientError = testingStuff.CallAfterRegisterSubscriptionConnection(_ => throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });

            using (db2.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var s = db2.SubscriptionStorage.DropSubscriptionConnections(state.SubscriptionId,
                    new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });
            }

            Console.WriteLine("");
            Thread.Sleep(Int32.MaxValue);

            transientError.Dispose();


















            using (Sharding.Resharding.ForTestingPurposesOnly().CallBetweenSourceAndDestinationMigrationCompletion(() =>
                   {
                       // ReSharper disable once AccessToDisposedClosure
                       /*using (var session = store.OpenSession())
                       {
                           session.Store(new User { Name = "EGR" }, $"num-322$users/1-A");
                           session.SaveChanges();
                       }*/
                   }))
            {
                // WaitForUserToContinueTheTest(store);

                 /*
                 DatabaseRecordWithEtag record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
               var bucket = Sharding.GetBucket(record.Sharding, id);
            var curShard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
        var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, curShard);
        */






























                await Sharding.Resharding.StartManualShardMigrationForId(store, lastId, Server, bucket, curShard, toShard);




                //Thread.Sleep(10_000);
                db1.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

                using (var session = store.OpenSession(sourceName))
                {
                    session.Store(new User { Name = "EGR" }, $"num-322$users/1-A");
                    session.SaveChanges();
                }
                var t = subscription.Run(batch =>
                                {
                                    foreach (var item in batch.Items)
                                    {
                                        if (users.Add(item.Id) == false)
                                        {

                                        }
                                    }
                                });
                while (users.Count != 11)
                {
                    Console.WriteLine("11: "+string.Join(",", users));
                    Thread.Sleep(333);
                }
                db1.ForTestingPurposesOnly().EnableWritesToTheWrongShard = false;
                WaitForUserToContinueTheTest(store);
                await Sharding.Resharding.StartManualShardMigrationForId(store, "num-322$users/1-A", Server, bucket, curShard, toShard);
                //Thread.Sleep(10_000);
            }
            while (users.Count != 12)
            {
                Console.WriteLine("12: " + string.Join(",", users));
                Thread.Sleep(333);
            }


        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce3()
        {
            using var store = Sharding.GetDocumentStore(
                new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.Sharding ??= new ShardingConfiguration() { Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } } };
                    }
                }
                );
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                }, "users/1-A");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    session.Store(new User { }, $"num-{i}$users/1-A");
                }
                session.SaveChanges();
            }

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
            {
                MaxDocsPerBatch = 1,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),

            }))
            {
                var mre = new AsyncManualResetEvent();
                var mre2 = new AsyncManualResetEvent();
                var mre3 = new AsyncManualResetEvent();
                var mre4 = new AsyncManualResetEvent();
                subscription.AfterAcknowledgment += async batch => 
                {
                    mre.Set();
                    await mre2.WaitAsync();


                    users.AddRange(batch.Items.Select(x=>x.Id));
                };

                var gotTwice = false;
                var twiceId = string.Empty;
                /*var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        Console.WriteLine($"processed: {item.Id} ");
                        if (users.Add(item.Id) == false)
                        {
                            Console.WriteLine($"Got exact same {item.Id} twice");
                            gotTwice = true;
                            twiceId = item.Id;
                        }
                    }
                });*/


              //  Assert.True(await mre.WaitAsync());

                //       var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                var shards =  await Sharding.GetShardsDocumentDatabaseInstancesFor(store).ToListAsync();
                //    var shard = shards.FirstAsync().Result.ShardedDocumentsStorage.GetCollectionDetails().ForTestingPurposesOnly();
                ShardedDocumentDatabase documentDatabase = null;
                foreach (var db in shards)
                {
                    documentDatabase = db;
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var ids = documentDatabase.DocumentsStorage.GetCollectionDetails(context, "Users").CountOfDocuments;
                        if (ids > 0)
                        {
                            break;
                        }
                    }
                }
                /*documentDatabase.TxMerger.Start();*/


                    Assert.NotNull(documentDatabase);
                var nextShard = (documentDatabase.ShardNumber + 1) % 2;
                var shard = shards.FirstOrDefault(x => x.ShardNumber == nextShard);
                Assert.NotNull(shard);

                IEnumerable<DynamicJsonValue> dynamicJsonValues1 = Cluster.GetRaftCommands(Server, nameof(RecordBatchSubscriptionDocumentsCommand));
                var ts = documentDatabase.ShardedDocumentsStorage.ForTestingPurposesOnly();
                //       var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var testingStuff = documentDatabase.ForTestingPurposesOnly();
                var transientError = testingStuff.CallAfterRegisterSubscriptionConnection(_ => throw new SubscriptionDoesNotBelongToNodeException($"DROPPED BY TEST") { AppropriateNode = null });
                using (ts.CallWhenDeleteBucket(id =>
                       {
                           mre4.Set();
                           mre3.WaitAsync().GetAwaiter().GetResult();
                       }))
                {
                    var bucket = await Sharding.Resharding.StartMovingShardForId(store, "users/1-A", nextShard, Servers.Count == 0 ? new List<RavenServer>() { Server } : Servers);
                    var t = subscription.Run(batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            //Console.WriteLine($"processed: {item.Id} ");
                            if (users.Add(item.Id) == false)
                            {
                         //       Console.WriteLine($"Got exact same {item.Id} twice");
                                gotTwice = true;
                                twiceId = item.Id;
                            }
                        }
                    });

               //     await mre.WaitAsync();
              //      await mre4.WaitAsync();
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "EGR" }, $"num-322$users/1-A");
                        session.SaveChanges();
                    }
                    mre3.Set();
                    transientError.Dispose();
   
                    var t11 = Sharding.Resharding.WaitForMigrationComplete(store, bucket);

                    await t11;
                    // here we started to delete document after resharding
                    // and the dest shard started to process docs
                    // we stop both operations
                    // and start source shard subscription
                    /*
                    var x1 = Cluster.GetRaftCommands(Server, nameof(RecordBatchSubscriptionDocumentsCommand)).Count();
                    transientError.Dispose();


                    Assert.True(await WaitForValueAsync(() =>
                    {
                        var x = Cluster.GetRaftCommands(Server, nameof(RecordBatchSubscriptionDocumentsCommand));
                        if (x.Count() > x1)
                        {
                            mre3.Set();
                            return true;
                        }

                        return false;
                    }, true));
                    */

                    mre2.Set();

                    /*Assert.Equal(11, await WaitForValueAsync(() => users.Count, 11, timeout: 15_000));*/
                    while (users.Count != 12)
                    {
                        Console.WriteLine(string.Join(",", users));
                        Thread.Sleep(333);
                    }
                }
            }
        }


        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocumentOnce2()
        {
            using var store = Sharding.GetDocumentStore(
                new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration() { Shards = new Dictionary<int, DatabaseTopology>() { { 0, new DatabaseTopology() }, { 1, new DatabaseTopology() } } };
                }
            }
                );

            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                }, "users/1-A");
                session.SaveChanges();
            }

            var adding = true;
            int? cc = null;
            var writes = Task.Run(() =>
            {
                var i = 0;
                while (adding)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { }, $"num-{i++}$users/1-A");
                        session.SaveChanges();
                    }
                }

                cc = i;
            });

            var sub = await store.Subscriptions.CreateAsync<User>();
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sub)
            {
                MaxDocsPerBatch = 6,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),

            }))
            {
                var gotTwice = false;
                var twiceId = string.Empty;
                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                  Console.WriteLine($"processed: {item.Id} # CV: {item.ChangeVector}");
                        if (users.Add(item.Id) == false)
                        {
                       //     Console.WriteLine($"Got exact same {item.Id} twice");
                            gotTwice = true;
                            twiceId = item.Id;
                        }
                    }
                });
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                /*await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");*/
                adding = false;
                await writes;



                try

                {
                    await t.WaitAsync(TimeSpan.FromSeconds(15));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                var zzz = WaitForValue(() =>
                {
                    if (cc == null)
                        return false;
                    if (cc + 1 != users.Count)
                    {
                        Console.WriteLine($"cc: {cc + 1} / users: {users.Count}");
                        return false;
                    }

                    return true;
                }, true, timeout: 5 * 60_000, interval: 1000);

                Assert.True(zzz, $"cc: {cc} / users: {users.Count}");
                Assert.False(gotTwice, "GotTwice " + twiceId);

                for (int i = 0; i < cc; i++)
                {
                    var id = $"num-{i}$users/1-A";
                    Assert.True(users.Contains(id), $"{id} is missing");
                    /*if (users.Contains(id) == false)
                    {
                        Console.WriteLine($"users Contains '{id}' == false");
                        Thread.Sleep(int.MaxValue);
                    }
                    else
                    {
                     //   Assert.True(users.Contains(id), $"{id} is missing");

                    }*/
                }

                /*if (users.Contains("users/1-A") == false)
                {
                    Console.WriteLine($"users Contains 'users/1-A' == false");
                    Thread.Sleep(int.MaxValue);
                }*/

                Assert.True(users.Contains("users/1-A"), "users/1-A is missing");
                Assert.Equal(cc + 1, users.Count);

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, sub);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications()
        {
            using var store = Sharding.GetDocumentStore();
            var docsCount = 100;
            using (var session = store.OpenAsyncSession())
            {
                await AddOrUpdateUserAsync(session, "users/1-A");
                await session.SaveChangesAsync();
            }
            var writes = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, "users/1-A");
                        await session.SaveChangesAsync();
                    }

                    for (int i = 3; i < docsCount; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A");
                            await AddOrUpdateUserAsync(session, $"users/{i}-A");
                            await session.SaveChangesAsync();
                        }
                    }
                }
            });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
            {
                MaxDocsPerBatch = 5,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            }))
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var timeoutEvent = new TimeoutEvent(TimeSpan.FromSeconds(5), "foo");
                timeoutEvent.Start(tcs.SetResult);

                var t = subscription.Run(batch =>
                {
                    timeoutEvent.Defer("Foo");
                    foreach (var item in batch.Items)
                    {
                        // Console.WriteLine($"Subscription got {item.Id} with age:{item.Result.Age}, cv: {item.ChangeVector}");

                        if (users.TryGetValue(item.Id, out var age))
                        {
                            if (Math.Abs(age) >= Math.Abs(item.Result.Age))
                            {
                                Debug.Assert(false, $"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                                throw new InvalidOperationException($"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                            }
                        }

                        users[item.Id] = item.Result.Age;
                    }
                });

                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                await writes;

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                    await WaitAndAssertForValueAsync(() => session.Query<User>().CountAsync(), (docsCount - 3) * 2 + 1);

                    var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                    foreach (var user in usersByQuery)
                    {
                        Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                        Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                        users.Remove(user.Id);
                    }
                }

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications2()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            await CreateItems(store, 0, 2);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await ProcessSubscription(store, id, users);
            await CreateItems(store, 6, 7);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8);
            await ProcessSubscription(store, id, users);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10);
            await ProcessSubscription(store, id, users);


            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task GetDocumentsWithFilteringAndModifications3()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            var t = ProcessSubscription(store, id, users, timoutSec: 15);
            await CreateItems(store, 0, 2);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 2, 4);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 4, 6);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 6, 7);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 7, 8);
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await CreateItems(store, 9, 10);

            await t;

            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);

                var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                foreach (var user in usersByQuery)
                {
                    Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                    Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                    users.Remove(user.Id);
                }
            }
        }

        private async Task ProcessSubscription(IDocumentStore store, string id, Dictionary<string, int> users, int timoutSec = 5)
        {
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
            {
                MaxDocsPerBatch = 5,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250),
                // CloseWhenNoDocsLeft = true
            }))
            {
                try
                {
                    var t = subscription.Run(batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            if (users.TryGetValue(item.Id, out var age))
                            {
                                if (Math.Abs(age) >= Math.Abs(item.Result.Age))
                                {
                                    throw new InvalidOperationException($"Got an outdated user {item.Id}, existing: {age}, received: {item.Result.Age}");
                                }
                            }

                            users[item.Id] = item.Result.Age;
                        }
                    });

                    await t.WaitAsync(TimeSpan.FromSeconds(timoutSec));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }


        private static async Task CreateItems(IDocumentStore store, int from, int to)
        {
            for (int j = from; j < to; j++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await AddOrUpdateUserAsync(session, "users/1-A");
                    await session.SaveChangesAsync();
                }

                for (int i = 3; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A");
                        await AddOrUpdateUserAsync(session, $"users/{i}-A");
                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        private static async Task AddOrUpdateUserAsync(IAsyncDocumentSession session, string id)
        {
            var current = await session.LoadAsync<User>(id);
            if (current == null)
            {
                current = new User();
                var age = Random.Shared.Next(1024);
                current.Age = age % 2 == 0 ? -1 : 1;
                await session.StoreAsync(current, id);
                return;
            }

            Assert.True(current.Age != 0);

            if (current.Age > 0)
                current.Age++;
            else
                current.Age--;

            current.Age *= -1;
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task ContinueSubscriptionAfterReshardingInACluster()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            });

            await SubscriptionWithResharding(store);
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions, Skip = "Need to stablize this")]
        public async Task ContinueSubscriptionAfterReshardingInAClusterWithFailover()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true, shouldRunInMemory: false);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
                RunInMemory = false
            });

            var id = await store.Subscriptions.CreateAsync<User>(predicate: u => u.Age > 0);
            var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var t = ProcessSubscription(store, id, users, timoutSec: 120);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var fail = Task.Run(async () =>
                {
                    int position = -1;
                    (string DataDirectory, string Url, string NodeTag) result = default;
                    var recoveryOptions = new ServerCreationOptions
                    {
                        RunInMemory = false,
                        DeletePrevious = false,
                        RegisterForDisposal = true,
                        CustomSettings = DefaultClusterSettings
                    };

                    recoveryOptions.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] =
                        cluster.Leader.Configuration.Cluster.ElectionTimeout.AsTimeSpan.ToString();

                    try
                    {
                        while (cts.IsCancellationRequested == false)
                        {
                            position = Random.Shared.Next(0, 5);
                            var node = cluster.Nodes[position];
                            if (node.ServerStore.IsLeader())
                                continue;

                            result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                            await Cluster.WaitForNodeToBeRehabAsync(store, result.NodeTag, token: cts.Token);

                            await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

                            cluster.Nodes[position] = await ReviveNodeAsync(result, recoveryOptions);
                            await Cluster.WaitForAllNodesToBeMembersAsync(store, token: cts.Token);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // shutdown
                    }
                });

                try
                {
                    await CreateItems(store, 0, 2);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    await CreateItems(store, 2, 4);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    await CreateItems(store, 4, 6);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    await CreateItems(store, 6, 7);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    await CreateItems(store, 7, 8);
                    await Sharding.Resharding.MoveShardForId(store, "users/1-A", servers: cluster.Nodes);
                    await CreateItems(store, 8, 10);
                }
                finally
                {
                    cts.Cancel();
                    await fail;
                    await t;
                }

                await Indexes.WaitForIndexingInTheClusterAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    var total = await session.Query<User>().CountAsync();
                    Assert.Equal(195, total);

                    var usersByQuery = await session.Query<User>().Where(u => u.Age > 0).ToListAsync();
                    foreach (var user in usersByQuery)
                    {
                        Assert.True(users.TryGetValue(user.Id, out var age), $"Missing {user.Id} from subscription");
                        Assert.True(age == user.Age, $"From sub:{age}, from shard: {user.Age} for {user.Id} cv:{session.Advanced.GetChangeVectorFor(user)}");
                        users.Remove(user.Id);
                    }
                }
            }
        }

        private async Task SubscriptionWithResharding(IDocumentStore store)
        {
            var id = await store.Subscriptions.CreateAsync<User>();
            var users = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var mre = new ManualResetEvent(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
            {
                MaxDocsPerBatch = 5,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
            }))
            {
                subscription.AfterAcknowledgment += batch =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        users.TryAdd(item.Id, new HashSet<string>(StringComparer.Ordinal));
                        var cv = users[item.Id];

                        if (cv.Add(item.ChangeVector) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }
                    }
                });


                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "foo$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "foo$users/8-A");

                    session.SaveChanges();
                }

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                mre.Reset();

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 1);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 1);

                await Sharding.Resharding.MoveShardForId(store, "users/1-A");
                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.True(false, "Worker completed without exception");
                }
                catch (TimeoutException)
                {
                    // expected, means the worker is still alive  
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "bar$users/1-A");
                    session.Store(new User(), "users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "bar$users/8-A");
                    session.Store(new User(), "users/8-A");

                    session.SaveChanges();
                }

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 2);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 2);

                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await Sharding.Resharding.MoveShardForId(store, "users/8-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "baz$users/1-A");
                    session.Store(new User(), "users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(), "baz$users/8-A");
                    session.Store(new User(), "users/8-A");


                    session.SaveChanges();
                }

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 3);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 3);
                await WaitForValueAsync(() => users.Count, 66);

                var expected = new HashSet<string>();
                for (int i = 1; i < 61; i++)
                {
                    var u = $"users/{i}-A";
                    expected.Add(u);
                }

                expected.Add("foo$users/1-A");
                expected.Add("bar$users/1-A");
                expected.Add("baz$users/1-A");
                expected.Add("foo$users/8-A");
                expected.Add("bar$users/8-A");
                expected.Add("baz$users/8-A");

                foreach (var user in users)
                {
                    expected.Remove(user.Key);
                }

                Assert.True(expected.Count == 0,
                    $"Missing {string.Join(Environment.NewLine, expected.Select(async e => $"{e} (shard: {await Sharding.GetShardNumberForAsync(store, e)})"))}");


                await Sharding.Subscriptions.AssertNoItemsInTheResendQueueAsync(store, id);
            }
        }

        private int _current;
        private string NextId => $"users/{Interlocked.Increment(ref _current)}-A";
    }
}

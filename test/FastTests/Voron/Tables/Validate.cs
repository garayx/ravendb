﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public class Validate : ReplicationTestBase
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public Validate(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        // the values are lower to make the cluster less stable
        protected override RavenServer GetNewServer(ServerCreationOptions options = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }
            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "10";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "3000";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50";

            return base.GetNewServer(options);
        }

        [Fact]
        public async Task ParallelClusterTransactions()
        {
            for (int i = 0; i < 100; i++)
            {
                _testOutputHelper.WriteLine($"ParallelClusterTransactions {i}");

                try
                {
                    await Internal();
                }
                catch (Exception e)
                {
                    _testOutputHelper.WriteLine($"Exception {e}");
                    throw;
                }
            }
        }

        private async Task Internal()
        {
            List<RavenServer> nods = null;
            string dbName = null;
            try
            {
                DebuggerAttachedTimeout.DisableLongTimespan = true;
                var numberOfNodes = 7;
                var cluster = await CreateRaftCluster(numberOfNodes);
                nods = cluster.Nodes;
                var db = GetDatabaseName();
                dbName = db;
                using (GetDocumentStore(new Options {Server = cluster.Leader, ReplicationFactor = numberOfNodes, ModifyDatabaseName = _ => db}))
                {
                    var count = 0;
                    var tasks = new List<Task>();
                    var random = new Random();

                    for (int i = 0; i < 100; i++)
                    {
                        var t = Task.Run(async () =>
                        {
                            var nodeNum = random.Next(0, numberOfNodes);
                            using (var store = GetDocumentStore(new Options {Server = cluster.Nodes[nodeNum], CreateDatabase = false}))
                            {
                                for (int j = 0; j < 10; j++)
                                {
                                    try
                                    {
                                        await store.Operations.ForDatabase(db)
                                            .SendAsync(new PutCompareExchangeValueOperation<User>($"usernames/{Interlocked.Increment(ref count)}", new User(), 0));

                                        using (var session = store.OpenAsyncSession(db))
                                        {
                                            session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                                            session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"usernames/{Interlocked.Increment(ref count)}", new User());
                                            await session.StoreAsync(new User());
                                            await session.SaveChangesAsync();
                                        }
                                    }
                                    catch
                                    {
                                        //
                                    }
                                }
                            }
                        });
                        tasks.Add(t);
                    }

                    foreach (var task in tasks)
                    {
                        try
                        {
                            await task;
                        }
                        catch
                        {
                            // ignore
                        }
                    }

                    var maxTerm = cluster.Nodes.Select(x => x.ServerStore.Engine.CurrentTerm).Max();
                    long maxTermOld;
                    var attempts = 3 * numberOfNodes;
                    do
                    {
                        maxTermOld = maxTerm;
                        await Task.Delay(TimeSpan.FromSeconds(3) * numberOfNodes);
                        maxTerm = cluster.Nodes.Select(x => x.ServerStore.Engine.CurrentTerm).Max();

                        attempts--; // cluster couldn't stabilize
                    } while (maxTerm != maxTermOld && attempts > 0);

                    var compareExchangeCount = new HashSet<long>();
                    var maxLog = 0L;

                    foreach (var n in cluster.Nodes.Where(x => x.ServerStore.Engine.CurrentTerm >= maxTerm))
                    {
                        using (n.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var currentLog = n.ServerStore.Engine.GetLastEntryIndex(ctx);
                            if (maxLog < currentLog)
                                maxLog = currentLog;
                        }
                    }

                    foreach (var node in cluster.Nodes)
                    {
                        await node.ServerStore.Cluster.WaitForIndexNotification(maxLog, TimeSpan.FromMinutes(1));

                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            compareExchangeCount.Add(node.ServerStore.Cluster.GetNumberOfCompareExchange(ctx, db));
                        }
                    }

                    Assert.Equal(1, compareExchangeCount.Count);
                }
            }
            catch (Exception e)
            {
                if (nods != null)
                {
                    var compareExchangeCount = new HashSet<long>();
                    foreach (var nod in nods)
                    {
                        using (nod.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var currentLog = nod.ServerStore.Engine.GetLastEntryIndex(ctx);
                            Console.WriteLine($"node: {nod.ServerStore.NodeTag}, index: {currentLog}");
                            compareExchangeCount.Add(nod.ServerStore.Cluster.GetNumberOfCompareExchange(ctx, dbName));
                        }
                    }

                    Assert.Equal(1, compareExchangeCount.Count);
                    _testOutputHelper.WriteLine($"compareExchangeCount.Count: {compareExchangeCount.Count}");
                    _testOutputHelper.WriteLine($"Exception {e}");
                }
                else
                {
                    Console.WriteLine($"nods null, dbName isNull? {dbName == null}");
                }

                _testOutputHelper.WriteLine($"Exception {e}");
                throw;
            }
        }
    }
}

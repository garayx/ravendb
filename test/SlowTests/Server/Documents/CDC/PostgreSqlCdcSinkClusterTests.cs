using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server;
using Raven.Server.Documents.CDC;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.CDC
{
    public class PostgreSqlCdcSinkClusterTests : CdcSinkTestBase
    {
        public PostgreSqlCdcSinkClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CanAddCdcSinkAndPerformInitialLoad(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_initial_load";
                var (state, _, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                Assert.True(state.Tables.All(x => x.InitialLoadCompleted == true), "Expected LSN to be set after initial load");

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.True(stats.CountOfDocuments >= 5, $"Expected at least 5 documents from initial load, got {stats.CountOfDocuments}");

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CanReplicateInsertFromPostgreSQL(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_insert";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('ClusterInsert')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool arrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "ClusterInsert");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(arrived, "Expected inserted document to arrive via CDC in cluster mode");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CanReplicateUpdateFromPostgreSQL(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_update";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.NotEqual("ClusterUpdated", customer.Firstname);
                }

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'ClusterUpdated' WHERE ""id"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool updated = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var customer = session.Load<Customer>("Customer/1");
                    return customer?.Firstname == "ClusterUpdated";
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(updated, "Expected the customer document to be updated via CDC in cluster mode");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CanReplicateDeleteFromPostgreSQL(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_delete";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert a fresh customer with no FK references so we can freely delete it
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('WillBeDeleted')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool inserted = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "WillBeDeleted");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(inserted, "Expected 'WillBeDeleted' customer to arrive before the delete");

                string insertedId;
                using (var session = store.OpenSession())
                {
                    var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    insertedId = session.Advanced.GetDocumentId(all.First(c => c.Firstname == "WillBeDeleted"));
                }

                var statsBeforeDelete = store.Maintenance.Send(new GetStatisticsOperation());

                var pgId = insertedId.Split('/')[1];
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {pgId}";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool deleted = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Load<Customer>(insertedId) == null;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(deleted, $"Expected {insertedId} to be deleted in RavenDB after DELETE in PostgreSQL");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CdcSinkFailoverWhenResponsibleNodeGoesDown(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3, RunInMemory = false };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_failover";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                // find the responsible node tag
                var responsibleTag = cdcDb.ServerStore.NodeTag;

                // dispose the responsible node
                var responsibleServer = nodes.First(s => s.ServerStore.NodeTag == responsibleTag);
                var disposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleServer);

                // wait for a new responsible node to pick up the CDC task
                string newResponsibleTag = null;
                var foundNewNode = await WaitForValueAsync(async () =>
                {
                    foreach (var server in nodes.Where(s => !s.Disposed))
                    {
                        try
                        {
                            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            if (database == null)
                                continue;

                            if (database.CdcSinkLoader == null)
                                continue;

                            var processState = GetCdcConfigState(database, config.CdcDocName);
                            if (processState.Tables.All(x => x.InitialLoadCompleted == true))
                            {
                                newResponsibleTag = server.ServerStore.NodeTag;
                                return true;
                            }
                        }
                        catch
                        {
                            // node might be in rehab, skip
                        }
                    }
                    return false;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(foundNewNode, $"Expected CDC task to failover to a new node after disposing node '{responsibleTag}'");
                Assert.NotEqual(responsibleTag, newResponsibleTag);

                // verify the new responsible node can process inserts from PostgreSQL
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('AfterFailover')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool arrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "AfterFailover");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(arrived, $"Expected 'AfterFailover' document to arrive via CDC on new responsible node '{newResponsibleTag}' after failover from '{responsibleTag}'");
            }
        }

        /// <summary>
        /// Scenario: The responsible node is disposed while it is actively consuming a CDC batch.
        /// Multiple rows are inserted into PostgreSQL and the node is killed immediately afterwards,
        /// before the CDC process has a chance to commit the LSN via Raft (UpdateProcessState).
        /// The new responsible node should re-consume from the old LSN and eventually deliver all documents.
        /// </summary>
        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_FailoverDuringActiveBatch_DocumentsAreEventuallyDelivered(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3, RunInMemory = false };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_failover_mid_batch";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                var responsibleTag = cdcDb.ServerStore.NodeTag;
                var lsnBeforeInserts = state.LastLsn;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // Insert multiple rows in a single transaction to create a batch
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);
                    for (int i = 0; i < 10; i++)
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.Transaction = tx;
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('MidBatch_{i}')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    await tx.CommitAsync(cts.Token);
                }

                // Kill the responsible node immediately — the CDC process may be mid-batch
                // or may not have updated its LSN via Raft yet
                var responsibleServer = nodes.First(s => s.ServerStore.NodeTag == responsibleTag);
                await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleServer);

                // Wait for a new node to pick up the CDC task
                var foundNewNode = await WaitForValueAsync(async () =>
                {
                    foreach (var server in nodes.Where(s => !s.Disposed))
                    {
                        try
                        {
                            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            if (database?.CdcSinkLoader == null)
                                continue;

                            var processState = GetCdcConfigState(database, config.CdcDocName);
                            if (processState.Tables.All(x => x.InitialLoadCompleted == true))
                                return true;
                        }
                        catch
                        {
                        }
                    }
                    return false;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(foundNewNode, "Expected CDC task to failover to a new node");

                // Verify all 10 documents eventually arrive via the new responsible node
                bool allArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var customers = session.Advanced.RawQuery<Customer>("from Customer where startsWith(Firstname, 'MidBatch_')").ToList();
                    return customers.Count == 10;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(allArrived, "Expected all 10 'MidBatch_*' documents to arrive after failover");
            }
        }

        /// <summary>
        /// Scenario: The responsible node processes CDC data and writes documents locally,
        /// but is killed before RavenDB's internal replication has time to propagate those
        /// documents to the other cluster nodes.
        /// 
        /// We verify that the surviving nodes eventually receive all the data — either
        /// through the new CDC responsible node re-consuming from the old LSN, or through
        /// delayed replication from the revived node.
        /// </summary>
        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_FailoverBeforeReplication_DocumentsAreEventuallyDelivered(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3, RunInMemory = false };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_failover_pre_repl";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                var responsibleTag = cdcDb.ServerStore.NodeTag;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // Insert rows and wait for them to arrive on the responsible node only
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    for (int i = 0; i < 5; i++)
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('PreRepl_{i}')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                }

                // Wait until the responsible node has the documents locally
                bool responsibleHasDocs = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var customers = session.Advanced.RawQuery<Customer>("from Customer where startsWith(Firstname, 'PreRepl_')").ToList();
                    return customers.Count == 5;
                }, true, timeout: 60_000, interval: 500);

                Assert.True(responsibleHasDocs, "Expected the responsible node to have received the 5 PreRepl_ documents via CDC");

                // Kill the responsible node immediately — replication to the other nodes may not be complete
                var responsibleServer = nodes.First(s => s.ServerStore.NodeTag == responsibleTag);
                var disposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleServer);

                // Use a store connected to one of the surviving nodes to verify data arrives
                var survivingServer = nodes.First(s => !s.Disposed);
                using var survivingStore = new DocumentStore
                {
                    Urls = new[] { survivingServer.WebUrl },
                    Database = store.Database,
                    Conventions = new DocumentConventions { DisableTopologyUpdates = true }
                }.Initialize();

                // The surviving node should eventually get all 5 documents,
                // either from the CDC task re-consuming on the new responsible node,
                // or from delayed replication when the killed node is eventually revived.
                bool survivorHasDocs = await WaitForValueAsync(() =>
                {
                    using var session = survivingStore.OpenSession();
                    var customers = session.Advanced.RawQuery<Customer>("from Customer where startsWith(Firstname, 'PreRepl_')").ToList();
                    return customers.Count == 5;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(survivorHasDocs, $"Expected the surviving node '{survivingServer.ServerStore.NodeTag}' to eventually have all 5 PreRepl_ documents after responsible node '{responsibleTag}' was killed");

                // Also insert new data to prove the CDC pipeline is still functional after failover
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('AfterPreReplFailover')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool newDocArrived = await WaitForValueAsync(() =>
                {
                    using var session = survivingStore.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "AfterPreReplFailover");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(newDocArrived, "Expected 'AfterPreReplFailover' document to arrive on the surviving node after failover");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task Cluster_CdcSinkFailoverWithNestedCollections(MigrationProvider provider)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3, RunInMemory = false };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Pre-insert categories and productcategory rows
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (1, 'Beverages')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (2, 'Condiments')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (1, 1)";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                }

                var collections = new List<Collection2>
                {
                    new Collection2
                    {
                        SourceTableName = "category", SourceTableSchema = schemaName, Name = "Category",
                        ColumnsMapping = new Dictionary<string, string> { { "name", "Name" } },
                        NestedCollections = new List<NestedCollection2>
                        {
                            new NestedCollection2
                            {
                                SourceTableName = "productcategory", SourceTableSchema = schemaName,
                                Name = "Productcategory",
                                JoinColumns = new List<string> { "categoryid" },
                                Type = RelationType.OneToMany,
                                ColumnsMapping = new Dictionary<string, string>(),
                                AttachmentNameMapping = new Dictionary<string, string>()
                            }
                        }
                    }
                };

                string configurationName = "cdc_cluster_nested_failover";
                var (state, cdcDb, config) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 2);

                // verify initial nested data arrived
                bool initialNested = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    return cat1?.Productcategory != null && cat1.Productcategory.Length == 1;
                }, true, timeout: 60_000, interval: 1000);
                Assert.True(initialNested, "Expected Category/1 to have 1 nested item after initial load");

                var responsibleTag = cdcDb.ServerStore.NodeTag;

                // dispose the responsible node
                var responsibleServer = nodes.First(s => s.ServerStore.NodeTag == responsibleTag);
                await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleServer);

                // wait for a new node to pick up the CDC task
                var foundNewNode = await WaitForValueAsync(async () =>
                {
                    foreach (var server in nodes.Where(s => !s.Disposed))
                    {
                        try
                        {
                            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            if (database?.CdcSinkLoader == null)
                                continue;

                            var processState = GetCdcConfigState(database, config.CdcDocName);
                            if (processState.Tables.All(x => x.InitialLoadCompleted == true))
                                return true;
                        }
                        catch
                        {
                        }
                    }
                    return false;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(foundNewNode, "Expected CDC task to failover to a new node");

                // insert a new nested row after failover
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (2, 1)";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                // Category/1 should now have 2 nested items
                bool nestedAfterFailover = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    return cat1?.Productcategory != null && cat1.Productcategory.Length == 2;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(nestedAfterFailover, "Expected Category/1 to have 2 nested items after CDC failover and new insert");
            }
        }
    }
}

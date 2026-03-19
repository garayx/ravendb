using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.CDC;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.CDC
{
    public class PostgreSqlCdcSinkClusterTests : CdcSinkClusterTestBase
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
            using (WithNpgSqlDatabase(out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_initial_load";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                Assert.True(state.LastLsn > 0, "Expected LSN to be set after initial load");

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
            using (WithNpgSqlDatabase(out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_insert";
                var (state, cdcDb) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

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
            using (WithNpgSqlDatabase(out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_update";
                var (state, cdcDb) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

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
            using (WithNpgSqlDatabase(out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var options = new Options { Server = leader, ReplicationFactor = 3 };
                using var store = GetDocumentStore(options);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                string configurationName = "cdc_cluster_delete";
                var (state, cdcDb) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

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
    }
}

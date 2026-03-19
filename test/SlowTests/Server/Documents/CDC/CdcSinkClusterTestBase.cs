using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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
using Raven.Client.Util;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.CDC
{
    [Trait("Category", "CdcSink")]
    public abstract class CdcSinkClusterTestBase : ClusterTestBase
    {
        private const int CommandTimeout = 10 * 60;

        protected CdcSinkClusterTestBase(ITestOutputHelper output) : base(output)
        {
        }

        internal DisposableAction WithNpgSqlDatabase(out string connectionString, out string schemaName, string dataSet = "northwind", bool includeData = true)
        {
            schemaName = "public";
            var databaseName = "npgSql_test_" + Guid.NewGuid();
            var rawConnectionString = NpgSqlConnectionString.Instance.VerifiedConnectionString.Value;
            connectionString = rawConnectionString + $";Database=\"{databaseName}\"";

            using (var connection = new NpgsqlConnection(rawConnectionString))
            {
                connection.Open();
                using (var dbCommand = connection.CreateCommand())
                {
                    dbCommand.CommandTimeout = CommandTimeout;
                    dbCommand.CommandText = $"CREATE DATABASE \"{databaseName}\"";
                    dbCommand.ExecuteNonQuery();
                }
                connection.Close();
            }

            if (string.IsNullOrEmpty(dataSet) == false)
            {
                using (var dbConnection = new NpgsqlConnection(connectionString))
                {
                    dbConnection.Open();
                    var assembly = GetType().Assembly;

                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandTimeout = CommandTimeout;
                        var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.npgsql." + dataSet + ".create.sql"));
                        dbCommand.CommandText = textStreamReader.ReadToEnd();
                        dbCommand.ExecuteNonQuery();
                    }

                    if (includeData)
                    {
                        using (var dbCommand = dbConnection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = CommandTimeout;
                            var dataStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.npgsql." + dataSet + ".insert.sql"));
                            dbCommand.CommandText = dataStreamReader.ReadToEnd();
                            dbCommand.ExecuteNonQuery();
                        }
                    }
                    dbConnection.Close();
                }
            }

            var dbName = databaseName;
            return new DisposableAction(() =>
            {
                using (var con = new NpgsqlConnection(rawConnectionString))
                {
                    con.Open();
                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandTimeout = CommandTimeout;
                        dbCommand.CommandText = $@"SELECT pg_terminate_backend(pg_stat_activity.pid)
                            FROM pg_stat_activity
                            WHERE pg_stat_activity.datname = '{dbName}'
                              AND pid <> pg_backend_pid();";
                        dbCommand.ExecuteNonQuery();

                        dbCommand.CommandText = $"DROP DATABASE IF EXISTS \"{dbName}\"";
                        dbCommand.ExecuteNonQuery();
                    }
                    con.Close();
                }
            });
        }

        protected (CdcConnectionString CdcConnectionString, string ConnectionStringName, MigrationSettings2 Settings) SetupCdcConfiguration(
            string connectionString, string schemaName, List<Collection2> collections = null)
        {
            int crazyGuid = new Random().Next();
            string connectionStringName = "NpgsqlCdcConnectionString";

            var sqlConnectionString = new PostgresqlConnectionSettings
            {
                ConnectionString = connectionString,
                FactoryName = nameof(SqlProvider.Npgsql),
                PostgresSlotName = $"rvn_cdc_slot_{crazyGuid}",
                PostgresPublicationName = $"rvn_cdc_pub_{crazyGuid}",
            };

            var cdcConnectionString = new CdcConnectionString
            {
                BrokerType = CdcBrokerType.PostgreSQL,
                Name = connectionStringName,
                PostgresqlConnectionSettings = sqlConnectionString
            };

            var settings = new MigrationSettings2()
            {
                BatchSize = 1001,
                Collections = collections ?? new List<Collection2>
                {
                    new Collection2 { SourceTableName = "customer", SourceTableSchema = schemaName, Name = "Customer",
                        ColumnsMapping = new Dictionary<string, string> { { "firstname", "Firstname" } } }
                }
            };

            return (cdcConnectionString, connectionStringName, settings);
        }

        protected async Task<(CdcSinkProcessState State, Raven.Server.Documents.DocumentDatabase Db)> SetupAndWaitForInitialLoad(
            DocumentStore store,
            Raven.Server.Documents.DocumentDatabase db,
            string connectionString,
            string schemaName,
            string configurationName,
            List<Collection2> collections = null,
            int expectedMinDocuments = 5)
        {
            var (cdcConnectionString, connectionStringName, settings) = SetupCdcConfiguration(connectionString, schemaName, collections);

            var result1 = store.Maintenance.Send(new PutConnectionStringOperation<CdcConnectionString>(cdcConnectionString));
            Assert.NotNull(result1.RaftCommandIndex);

            var config = new CdcSinkConfiguration
            {
                Name = configurationName,
                ConnectionStringName = connectionStringName,
                Scripts = null,
                BrokerType = CdcBrokerType.PostgreSQL,
                Settings = settings
            };

            store.Maintenance.Send(new AddCdcSinkOperation<SqlConnectionString>(config));

            // In a cluster, the CDC task runs on a specific node. Find the right database instance.
            Raven.Server.Documents.DocumentDatabase cdcDb = null;
            CdcSinkProcessState state = null;
            var res = await WaitForValueAsync(async () =>
            {
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    if (database == null)
                        continue;

                    var processState = CdcSinkProcess.GetProcessState(database, configurationName);
                    if (processState.LastLsn > 0)
                    {
                        cdcDb = database;
                        state = processState;
                        return true;
                    }
                }
                return false;
            }, true, timeout: 60_000, interval: 1000);

            Assert.True(res, "Expected CDC process to start on one of the cluster nodes");

            if (expectedMinDocuments > 0)
            {
                bool initialLoadDone = await WaitForValueAsync(() =>
                {
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    return stats.CountOfDocuments >= expectedMinDocuments;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(initialLoadDone, $"Expected at least {expectedMinDocuments} documents from initial load");
            }

            return (state, cdcDb);
        }

        protected async Task WaitForLsnAdvance(Raven.Server.Documents.DocumentDatabase db, string configurationName, ulong previousLsn, int timeout = 60_000)
        {
            Assert.True(await WaitForValueAsync(() =>
            {
                try
                {
                    var state = CdcSinkProcess.GetProcessState(db, configurationName);
                    return state.LastLsn > previousLsn;
                }
                catch
                {
                    return false;
                }
            }, true, timeout: timeout, interval: 1000), $"Expected LSN to advance beyond {previousLsn}");
        }

        protected async Task AdvanceCustomerSequence(string connectionString, string schemaName, CancellationToken token)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync(token);
                using (var seqCmd = conn.CreateCommand())
                {
                    seqCmd.CommandText = $@"SELECT setval(pg_get_serial_sequence('""{schemaName}"".""customer""', 'id'), MAX(id)) FROM ""{schemaName}"".""customer""";
                    await seqCmd.ExecuteScalarAsync(token);
                }
            }
        }
    }
}

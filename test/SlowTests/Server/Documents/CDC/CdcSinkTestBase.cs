using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Util;
using Raven.Server.Documents.CDC;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace SlowTests.Server.Documents.CDC
{
    [Trait("Category", "CdcSink")]
    public abstract class CdcSinkTestBase : ClusterTestBase
    {
        private const int CommandTimeout = 10 * 60;

        protected CdcSinkTestBase(ITestOutputHelper output) : base(output)
        {
            QueueSuffix = Guid.NewGuid().ToString("N");
        }

        protected string QueueSuffix { get; }

        protected string UsersQueueName => $"users{QueueSuffix}";

        protected List<string> DefaultQueues => new() { UsersQueueName };

        internal DisposableAction WithSqlDatabase(MigrationProvider provider, out string connectionString, out string schemaName, string dataSet = "northwind", bool includeData = true)
        {
            if (provider != MigrationProvider.NpgSQL)
                throw new NotSupportedException($"CDC sink tests only support NpgSQL provider, got: {provider}");

            schemaName = "public";
            return WithNpgSqlDatabase(out connectionString, dataSet, includeData);
        }

        private DisposableAction WithNpgSqlDatabase(out string connectionString, string dataSet, bool includeData)
        {
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

        protected async Task<(PostgresqlCdcSink.Config State, Raven.Server.Documents.DocumentDatabase Db, CdcSinkConfiguration Config)> SetupAndWaitForInitialLoad(
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

            Raven.Server.Documents.DocumentDatabase cdcDb = null;
            PostgresqlCdcSink.Config state = null;

            // In a cluster, the CDC task runs on a specific node — iterate all servers.
            // In single-node mode, Servers may be empty — fall back to the passed db instance.
            var serversToCheck = Servers.Count > 0 ? Servers : null;

            var res = await WaitForValueAsync(async () =>
            {
                if (serversToCheck != null)
                {
                    foreach (var server in serversToCheck)
                    {
                        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        if (database == null)
                            continue;

                        var processState = GetCdcConfigState(database, config.CdcDocName);
                        if (state.Tables.All(x => x.InitialLoadCompleted == true))
                        {
                            cdcDb = database;
                            state = processState;
                            return true;
                        }
                    }
                }
                else
                {
                    var processState = GetCdcConfigState(db, config.CdcDocName);
                    if (state.Tables.All(x => x.InitialLoadCompleted == true))
                    {
                        cdcDb = db;
                        state = processState;
                        return true;
                    }
                }
                return false;
            }, true, timeout: 60_000, interval: 1000);

            Assert.True(res, "Expected CDC process to start");

            if (expectedMinDocuments > 0)
            {
                bool initialLoadDone = await WaitForValueAsync(() =>
                {
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    return stats.CountOfDocuments >= expectedMinDocuments;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(initialLoadDone, $"Expected at least {expectedMinDocuments} documents from initial load");
            }

            return (state, cdcDb, config);
        }


        protected PostgresqlCdcSink.Config GetCdcConfigState(Raven.Server.Documents.DocumentDatabase database, string cdcConfigId)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {

                BlittableJsonReaderObject data = null;
                    data = database.DocumentsStorage.Get(context, cdcConfigId)?.Data;

                return data == null ? new PostgresqlCdcSink.Config() : JsonDeserializationServer.PostgresqlCdcSinkConfig(data);
            }
        }

        protected async Task WaitForLsnAdvance(Raven.Server.Documents.DocumentDatabase db, string cdcDocName, ulong previousLsn, int timeout = 60_000)
        {
            Assert.True(await WaitForValueAsync(() =>
            {
                try
                {
                    var state = GetCdcConfigState(db, cdcDocName);
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

        protected async Task AdvanceTableSequence(string connectionString, string schemaName, string tableName, string pkColumn, CancellationToken token)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync(token);
                using (var seqCmd = conn.CreateCommand())
                {
                    seqCmd.CommandText = $@"SELECT setval(pg_get_serial_sequence('""{schemaName}"".""{tableName}""', '{pkColumn}'), MAX(""{pkColumn}"")) FROM ""{schemaName}"".""{tableName}""";
                    await seqCmd.ExecuteScalarAsync(token);
                }
            }
        }

        protected AddCdcSinkOperationResult AddCdcSink<T>(DocumentStore src, CdcSinkConfiguration configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddCdcSinkOperation<T>(configuration));
            return addResult;
        }

        private async Task<string[]> GetCdcSinkErrorNotifications(DocumentStore src)
        {
            var databaseInstanceFor = await Databases.GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                var notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("CdcSink_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
                return notifications;
            }
        }
        
        public async Task<CdcSinkErrorInfo> TryErrorFromAlertAsync(string databaseName, CdcSinkConfiguration config)
        {
            return null;
        }
        
        protected AsyncManualResetEvent WaitForCdcSinkBatch(DocumentStore store,
            Func<string, CdcSinkProcessStatistics, bool> predicate)
        {
            var database = AsyncHelpers.RunSync(() => GetDatabase(store.Database));

            var amre = new AsyncManualResetEvent();

            database.CdcSinkLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.ScriptName}", x.Statistics))
                    amre.Set();
            };

            return amre;
        }

        protected async Task AssertCdcSinkDoneAsync(AsyncManualResetEvent etlDone, TimeSpan timeout, string databaseName, CdcSinkConfiguration config)
        {
            if (await etlDone.WaitAsync(timeout) == false)
            {
                var error = AsyncHelpers.RunSync(() => TryErrorFromAlertAsync(databaseName, config));

                Assert.Fail($"Queue Sink wasn't done. Error: {error?.Error}");
            }
        }

        protected class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }

            public string FullName { get; set; }
        }
    }
}

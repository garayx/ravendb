using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.CDC;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using SlowTests.Server.Documents.Attachments;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.CDC
{
    public partial class PostgreSqlCdcSinkTests : CdcSinkTestBase
    {
        public PostgreSqlCdcSinkTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanSimpleImport_OneToOne(MigrationProvider provider)
        {
            // TODO: egor lets see if we want to use this data set in the future
            //var dest = "postgresql_northwind";
            //var snapshot = $"{dest}.zip";
            //var backupPath = NewDataPath(forceCreateDir: true);
            //var fullBackupPath = Path.Combine(backupPath, snapshot);
            //var sqlImportFile = Path.Combine(backupPath, $"{dest}.sql");

            //await using (var file = File.Create(fullBackupPath))
            //{
            //    await using (var stream = typeof(PostgreSqlCdcSinkTests).Assembly.GetManifestResourceStream($"SlowTests.Data.CDC.{snapshot}"))
            //    {
            //        Assert.NotNull(stream);
            //        await stream.CopyToAsync(file);
            //    }
            //}

            //var zipPath = new PathSetting(fullBackupPath);
            //Assert.True(File.Exists(zipPath.FullPath));

            //await ZipFile.ExtractToDirectoryAsync(zipPath.FullPath, backupPath);

            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                //      var json = @"{          ""BatchSize"": 1001,             ""Collections"": [{                     ""SourceTableName"": ""nopktable"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Nopktable"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""id"": ""Id""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""unsupportedtable"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Unsupportedtable"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {},                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""customer"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Customer"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""firstname"": ""Firstname""                     },                     ""AttachmentNameMapping"": {                         ""pic"": ""Pic""                     }                 }, {                     ""SourceTableName"": ""category"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Category"",                     ""Patch"": ""// flatten many-to-many relationship\r\nthis.Productcategory = this.Productcategory.map(x => x.Productid);\r\n"",                     ""NestedCollections"": [{                             ""Name"": ""Productcategory"",                             ""SourceTableSchema"": ""public"",                             ""SourceTableName"": ""productcategory"",                             ""Type"": ""OneToMany"",                             ""JoinColumns"": [""categoryid""],                             ""ColumnsMapping"": {},                             ""AttachmentNameMapping"": {},                             ""LinkedCollections"": [{                                     ""Name"": ""Productid"",                                     ""SourceTableName"": ""product"",                                     ""SourceTableSchema"": ""public"",                                     ""JoinColumns"": [""productid""],                                     ""Type"": ""ManyToOne""                                 }                             ],                             ""NestedCollections"": [],                             ""SqlKeysStorage"": ""None""                         }                     ],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""name"": ""Name""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""Order"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Order"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Customerid"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""customerid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""orderdate"": ""Orderdate"",                         ""totalamount"": ""Totalamount""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""orderitem"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Orderitem"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Orderid"",                             ""SourceTableName"": ""Order"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""orderid""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Productid"",                             ""SourceTableName"": ""product"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""productid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""unitprice"": ""Unitprice""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""details"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Details"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""OrderidAndProductid"",                             ""SourceTableName"": ""orderitem"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""orderid"", ""productid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""name"": ""Name""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""product"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Product"",                     ""Patch"": ""// flatten many-to-many relationship\r\nthis.Productcategory = this.Productcategory.map(x => x.Categoryid);\r\n"",                     ""NestedCollections"": [{                             ""Name"": ""Productcategory"",                             ""SourceTableSchema"": ""public"",                             ""SourceTableName"": ""productcategory"",                             ""Type"": ""OneToMany"",                             ""JoinColumns"": [""productid""],                             ""ColumnsMapping"": {},                             ""AttachmentNameMapping"": {},                             ""LinkedCollections"": [{                                     ""Name"": ""Categoryid"",                                     ""SourceTableName"": ""category"",                                     ""SourceTableSchema"": ""public"",                                     ""JoinColumns"": [""categoryid""],                                     ""Type"": ""ManyToOne""                                 }                             ],                             ""NestedCollections"": [],                             ""SqlKeysStorage"": ""None""                         }                     ],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""unitprice"": ""Unitprice"",                         ""isdiscontinued"": ""Isdiscontinued""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""photo"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Photo"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Photographer"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""photographer""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Inpic1"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""inpic1""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Inpic2"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""inpic2""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {},                     ""AttachmentNameMapping"": {                         ""pic"": ""Pic""                     }                 }             ]         } ";
                var json =
                    @"{          ""BatchSize"": 1001,             ""Collections"": [{                     ""SourceTableName"": ""nopktable"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Nopktable"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""id"": ""Id""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""unsupportedtable"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Unsupportedtable"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {},                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""customer"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Customer"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""firstname"": ""Firstname""                     },                     ""AttachmentNameMapping"": {                         ""pic"": ""Pic""                     }                 }, {                     ""SourceTableName"": ""category"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Category"",                     ""Patch"": ""// flatten many-to-many relationship\r\nthis.Productcategory = this.Productcategory.map(x => x.Productid);\r\n"",                     ""NestedCollections"": [{                             ""Name"": ""Productcategory"",                             ""SourceTableSchema"": ""public"",                             ""SourceTableName"": ""productcategory"",                             ""Type"": ""OneToMany"",                             ""JoinColumns"": [""categoryid""],                             ""ColumnsMapping"": {},                             ""AttachmentNameMapping"": {},                             ""LinkedCollections"": [{                                     ""Name"": ""Productid"",                                     ""SourceTableName"": ""product"",                                     ""SourceTableSchema"": ""public"",                                     ""JoinColumns"": [""productid""],                                     ""Type"": ""ManyToOne""                                 }                             ],                             ""NestedCollections"": [],                             ""SqlKeysStorage"": ""None""                         }                     ],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""name"": ""Name""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""Order"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Order"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Customerid"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""customerid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""orderdate"": ""Orderdate"",                         ""totalamount"": ""Totalamount""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""orderitem"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Orderitem"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Orderid"",                             ""SourceTableName"": ""Order"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""orderid""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Productid"",                             ""SourceTableName"": ""product"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""productid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""unitprice"": ""Unitprice""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""details"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Details"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""OrderidAndProductid"",                             ""SourceTableName"": ""orderitem"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""orderid"", ""productid""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {                         ""name"": ""Name""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""product"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Product"",                     ""Patch"": ""// flatten many-to-many relationship\r\nthis.Productcategory = this.Productcategory.map(x => x.Categoryid);\r\n"",                     ""NestedCollections"": [{                             ""Name"": ""Productcategory"",                             ""SourceTableSchema"": ""public"",                             ""SourceTableName"": ""productcategory"",                             ""Type"": ""OneToMany"",                             ""JoinColumns"": [""productid""],                             ""ColumnsMapping"": {},                             ""AttachmentNameMapping"": {},                             ""LinkedCollections"": [{                                     ""Name"": ""Categoryid"",                                     ""SourceTableName"": ""category"",                                     ""SourceTableSchema"": ""public"",                                     ""JoinColumns"": [""categoryid""],                                     ""Type"": ""ManyToOne""                                 }                             ],                             ""NestedCollections"": [],                             ""SqlKeysStorage"": ""None""                         }                     ],                     ""LinkedCollections"": [],                     ""ColumnsMapping"": {                         ""unitprice"": ""Unitprice"",                         ""isdiscontinued"": ""Isdiscontinued""                     },                     ""AttachmentNameMapping"": {}                 }, {                     ""SourceTableName"": ""photo"",                     ""SourceTableSchema"": ""public"",                     ""Name"": ""Photo"",                     ""NestedCollections"": [],                     ""LinkedCollections"": [{                             ""Name"": ""Photographer"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""photographer""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Inpic1"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""inpic1""],                             ""Type"": ""ManyToOne""                         }, {                             ""Name"": ""Inpic2"",                             ""SourceTableName"": ""customer"",                             ""SourceTableSchema"": ""public"",                             ""JoinColumns"": [""inpic2""],                             ""Type"": ""ManyToOne""                         }                     ],                     ""ColumnsMapping"": {},                     ""AttachmentNameMapping"": {                         ""pic"": ""Pic""                     }                 }             ]         } ";
                BlittableJsonReaderObject reader = context.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(json)), "users/1");
                var serializer = DocumentConventions.DefaultForServer.Serialization.CreateDeserializer();



                MigrationSettings settings = null;
                using (var blittableJsonReader = new BlittableJsonReader())
                {
                    blittableJsonReader.Initialize(reader);
                    settings = serializer.Deserialize<MigrationSettings>(blittableJsonReader);
                }

                Assert.NotNull(settings);

                var settings2 = new MigrationSettings2()
                {
                    BatchSize = settings.BatchSize,
                    Collections = settings.Collections.Select(x => new Collection2()
                    {
                        SourceTableName = x.SourceTableName,
                        ColumnsMapping = x.ColumnsMapping,
                        SourceTableSchema = x.SourceTableSchema,
                        Name = x.Name,
                        Patch = x.Patch
                    }).ToList(),
                };

                int crazyGuid = new Random().Next();
                var connectionStringName = "NpgsqlCdcConnectionString";
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

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<CdcConnectionString>(cdcConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);

                string configurationName = "my first cdc with postgresql";
                var config = new CdcSinkConfiguration
                {
                    Name = configurationName ?? connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Scripts = null,
                    BrokerType = CdcBrokerType.PostgreSQL,
                    Settings = settings2
                };

                var addResult = store.Maintenance.Send(new AddCdcSinkOperation<SqlConnectionString>(config));
                // assert initial load
                // assert can add new doc via logical replication

                CdcSinkProcessState state = null;
                ulong lsnBeforeInsert = ulong.MaxValue;
                var res = await WaitForValueAsync(() =>
                {
                    state = CdcSinkProcess.GetProcessState(db, configurationName);
                    lsnBeforeInsert = state.LastLsn;
                    return state.LastLsn > 0;
                }, true, timeout: 60_000, interval: 1000);


                Assert.NotNull(state);
                Assert.True(res);

                DatabaseStatistics stats;
                using (var session = store.OpenSession())
                {
                    stats = store.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(25, stats.CountOfDocuments);
                }

                // now lets add a new document to postgresql
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    // the northwind insert.sql uses explicit id values, so the serial sequence is out of sync -
                    // advance it to the current max before inserting to avoid a duplicate key violation
                    using (var seqCmd = conn.CreateCommand())
                    {
                        seqCmd.CommandText = $@"SELECT setval(pg_get_serial_sequence('""{schemaName}"".""customer""', 'id'), MAX(id)) FROM ""{schemaName}"".""customer""";
                        await seqCmd.ExecuteScalarAsync(cts.Token);
                    }

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('CdcTest')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }
                // now lets add a new document to postgresql
                //            using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                //            {
                //                await conn.OpenAsync(cts.Token);

                //                using var cmd = conn.CreateCommand();
                //                cmd.CommandText = $@"
                //    INSERT INTO ""{schemaName}"".""customer"" (""firstname"", ""lastname"", ""address"", ""city"", ""postalcode"", ""country"", ""phone"")
                //    VALUES ('CdcTest', 'User', '123 CDC St', 'TestCity', '00000', 'TestCountry', '555-0000')
                //";
                //                await cmd.ExecuteNonQueryAsync(cts.Token);
                //            }

                Console.WriteLine();
                Console.WriteLine("$$$ WAIT "+store.Urls.FirstOrDefault());

                CdcSinkProcessState stateAfterInsert = null;
                Assert.True(await WaitForValueAsync(() =>
                {
                    stateAfterInsert = CdcSinkProcess.GetProcessState(db, configurationName);
                    return stateAfterInsert.LastLsn > lsnBeforeInsert;
                }, true, timeout: 60_000, interval: 1000), "Expected LSN to advance after inserting into PostgreSQL via CDC");

                bool newDocArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments > stats.CountOfDocuments;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(newDocArrived, "Expected a new document to arrive after inserting into PostgreSQL via CDC");

                var updatedStats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(26, updatedStats.CountOfDocuments);


                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/6");
                    Assert.Equal("CdcTest", customer.Firstname);
                }
            }
        }
        private class CustomerWithLastname
        {
            public string Firstname { get; set; }
            public string Lastname { get; set; }
        }

        private (CdcConnectionString CdcConnectionString, string ConnectionStringName, MigrationSettings2 Settings) SetupCdcConfiguration(
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

        private async Task WaitForLsnAdvance(Raven.Server.Documents.DocumentDatabase db, string configurationName, ulong previousLsn, int timeout = 60_000)
        {
            Assert.True(await WaitForValueAsync(() =>
            {
                var state = CdcSinkProcess.GetProcessState(db, configurationName);
                return state.LastLsn > previousLsn;
            }, true, timeout: timeout, interval: 1000), $"Expected LSN to advance beyond {previousLsn}");
        }

        private async Task<(CdcSinkProcessState State, Raven.Server.Documents.DocumentDatabase Db)> SetupAndWaitForInitialLoad(
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

            CdcSinkProcessState state = null;
            var res = await WaitForValueAsync(() =>
            {
                state = CdcSinkProcess.GetProcessState(db, configurationName);
                return state.LastLsn > 0;
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

            return (state, db);
        }

        private async Task AdvanceCustomerSequence(string connectionString, string schemaName, CancellationToken token)
        {
            using (var conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync(token);
                using (var seqCmd = conn.CreateCommand())
                {
                    seqCmd.CommandText = $@"SELECT setval(pg_get_serial_sequence('""{schemaName}"".""customer""', 'id'), MAX(id)) FROM ""{schemaName}"".""customer""";
                    await seqCmd.ExecuteScalarAsync(token);
                }
            }
        }

        private async Task AdvanceTableSequence(string connectionString, string schemaName, string tableName, string pkColumn, CancellationToken token)
        {
            using (var conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync(token);
                using (var seqCmd = conn.CreateCommand())
                {
                    seqCmd.CommandText = $@"SELECT setval(pg_get_serial_sequence('""{schemaName}"".""{tableName}""', '{pkColumn}'), MAX(""{pkColumn}"")) FROM ""{schemaName}"".""{tableName}""";
                    await seqCmd.ExecuteScalarAsync(token);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateFromEmptyDatabase(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: false))
            {
                var (cdcConnectionString, connectionStringName, settings) = SetupCdcConfiguration(connectionString, schemaName);

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<CdcConnectionString>(cdcConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);

                string configurationName = "cdc_empty_db_test";
                var config = new CdcSinkConfiguration
                {
                    Name = configurationName,
                    ConnectionStringName = connectionStringName,
                    Scripts = null,
                    BrokerType = CdcBrokerType.PostgreSQL,
                    Settings = settings
                };

                store.Maintenance.Send(new AddCdcSinkOperation<SqlConnectionString>(config));

                CdcSinkProcessState state = null;
                var res = await WaitForValueAsync(() =>
                {
                    state = CdcSinkProcess.GetProcessState(db, configurationName);
                    return state.LastLsn > 0;
                }, true, timeout: 60_000, interval: 1000);

                Assert.NotNull(state);
                Assert.True(res, "Expected CDC process to start on empty database");

                var initialStats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(0, initialStats.CountOfDocuments);

                ulong lsnBeforeInsert = state.LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Alice')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInsert);

                bool newDocArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments > 0;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(newDocArrived, "Expected a new document to arrive via CDC from empty database");

                var updatedStats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, updatedStats.CountOfDocuments);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.Equal("Alice", customer.Firstname);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanInitialReplicateThenLogicalReplicateWithSchemaChange(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_schema_change_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                DatabaseStatistics statsBeforeAlter = store.Maintenance.Send(new GetStatisticsOperation());
                long previousCount = statsBeforeAlter.CountOfDocuments;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using (var alterCmd = conn.CreateCommand())
                    {
                        alterCmd.CommandText = $@"ALTER TABLE ""{schemaName}"".""customer"" ADD COLUMN ""lastname"" varchar(40)";
                        await alterCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"", ""lastname"") VALUES ('Bob', 'Smith')";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }
                }

                bool newDocArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments > previousCount;
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(newDocArrived, "Expected a new document to arrive after schema change and insert");

                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    var bob = allCustomers.FirstOrDefault(c => c.Firstname == "Bob");
                    Assert.NotNull(bob);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true, Skip = "Requires investigation into CDC process state persistence across database disable/enable cycle")]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateAfterDatabaseDisableAndEnable(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_disable_enable_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                DatabaseStatistics statsBeforeDisable = store.Maintenance.Send(new GetStatisticsOperation());

                // disable the database
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                await Task.Delay(2000);

                // re-enable the database
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));

                // re-acquire the database instance (it was recreated after enable)
                db = await Databases.GetDocumentDatabaseInstanceFor(store);

                CdcSinkProcessState stateAfterEnable = null;
                var resumed = await WaitForValueAsync(() =>
                {
                    stateAfterEnable = CdcSinkProcess.GetProcessState(db, configurationName);
                    return stateAfterEnable.LastLsn > 0;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(resumed, "Expected CDC process to resume after database re-enable");

                ulong lsnBeforeInsert = stateAfterEnable.LastLsn;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Charlie')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInsert);

                long countBeforeInsert = statsBeforeDisable.CountOfDocuments;
                bool newDocArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments > countBeforeInsert;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(newDocArrived, "Expected a new document to arrive after database disable/enable cycle");

                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    var charlie = allCustomers.FirstOrDefault(c => c.Firstname == "Charlie");
                    Assert.NotNull(charlie);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateUpdateFromPostgreSQL(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_update_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                // verify the original value
                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.NotEqual("UpdatedName", customer.Firstname);
                }

                ulong lsnBeforeUpdate = state.LastLsn;

                // update an existing customer
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'UpdatedName' WHERE ""id"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeUpdate);

                // wait for the update to be reflected in RavenDB
                bool updated = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var customer = session.Load<Customer>("Customer/1");
                        return customer?.Firstname == "UpdatedName";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updated, "Expected the customer document to be updated via CDC");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateMultipleInsertsInSingleTransaction(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_multi_insert_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                DatabaseStatistics statsBeforeInsert = store.Maintenance.Send(new GetStatisticsOperation());
                ulong lsnBeforeInsert = state.LastLsn;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert multiple rows in a single transaction
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var cmd1 = conn.CreateCommand())
                    {
                        cmd1.Transaction = tx;
                        cmd1.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Tx1')";
                        await cmd1.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var cmd2 = conn.CreateCommand())
                    {
                        cmd2.Transaction = tx;
                        cmd2.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Tx2')";
                        await cmd2.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var cmd3 = conn.CreateCommand())
                    {
                        cmd3.Transaction = tx;
                        cmd3.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Tx3')";
                        await cmd3.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInsert);

                // wait for all 3 new documents to arrive
                bool allArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments >= statsBeforeInsert.CountOfDocuments + 3;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(allArrived, "Expected all 3 documents from the single transaction to arrive via CDC");

                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    Assert.NotNull(allCustomers.FirstOrDefault(c => c.Firstname == "Tx1"));
                    Assert.NotNull(allCustomers.FirstOrDefault(c => c.Firstname == "Tx2"));
                    Assert.NotNull(allCustomers.FirstOrDefault(c => c.Firstname == "Tx3"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateMultipleCollections(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                var collections = new List<Collection2>
                {
                    new Collection2
                    {
                        SourceTableName = "customer", SourceTableSchema = schemaName, Name = "Customer",
                        ColumnsMapping = new Dictionary<string, string> { { "firstname", "Firstname" } }
                    },
                    new Collection2
                    {
                        SourceTableName = "product", SourceTableSchema = schemaName, Name = "Product",
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "unitprice", "Unitprice" },
                            { "isdiscontinued", "Isdiscontinued" }
                        }
                    }
                };

                string configurationName = "cdc_multi_collection_test";
                // northwind has 5 customers + 4 products = 9 minimum documents
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 9);

                ulong lsnBeforeInserts = state.LastLsn;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);
                await AdvanceTableSequence(connectionString, schemaName, "product", "id", cts.Token);

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    // insert a new customer
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('MultiCollTest')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    // insert a new product
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""product"" (""unitprice"", ""isdiscontinued"") VALUES (99.99, false)";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInserts);

                DatabaseStatistics statsAfter = null;
                bool bothArrived = await WaitForValueAsync(() =>
                {
                    statsAfter = store.Maintenance.Send(new GetStatisticsOperation());
                    return statsAfter.CountOfDocuments >= 11; // 9 initial + 2 new
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(bothArrived, $"Expected at least 11 documents, got {statsAfter?.CountOfDocuments}");

                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    Assert.NotNull(allCustomers.FirstOrDefault(c => c.Firstname == "MultiCollTest"));

                    var allProducts = session.Advanced.RawQuery<Product>("from Product").ToList();
                    Assert.Contains(allProducts, p => p.Unitprice == 99.99m);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateUpdateAndInsertInSameTransaction(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_update_insert_tx_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                DatabaseStatistics statsBeforeTx = store.Maintenance.Send(new GetStatisticsOperation());
                ulong lsnBeforeTx = state.LastLsn;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // perform update + insert in a single transaction
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var updateCmd = conn.CreateCommand())
                    {
                        updateCmd.Transaction = tx;
                        updateCmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'ModifiedInTx' WHERE ""id"" = 1";
                        await updateCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.Transaction = tx;
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('InsertedInTx')";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeTx);

                // verify both the update and the insert
                bool updateReflected = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var customer = session.Load<Customer>("Customer/1");
                        return customer?.Firstname == "ModifiedInTx";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updateReflected, "Expected customer 1 to be updated to 'ModifiedInTx'");

                bool insertArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments > statsBeforeTx.CountOfDocuments;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(insertArrived, "Expected the new customer inserted in the same transaction to arrive");

                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    Assert.NotNull(allCustomers.FirstOrDefault(c => c.Firstname == "InsertedInTx"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true, Skip = "Requires investigation into CDC process state persistence across database disable/enable cycle")]
        [RequiresNpgSqlInlineData]
        public async Task DoesNotDuplicateDocumentsAfterLsnResume(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_no_duplicate_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                DatabaseStatistics statsAfterInitialLoad = store.Maintenance.Send(new GetStatisticsOperation());
                long initialCount = statsAfterInitialLoad.CountOfDocuments;

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert one row and wait for it to arrive
                ulong lsnBeforeFirst = state.LastLsn;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('FirstInsert')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeFirst);

                bool firstArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments == initialCount + 1;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(firstArrived, "Expected first insert to arrive");

                // disable and re-enable the database to force CDC process restart
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                await Task.Delay(2000);
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // wait for CDC to resume
                CdcSinkProcessState stateAfterRestart = null;
                var resumed = await WaitForValueAsync(() =>
                {
                    stateAfterRestart = CdcSinkProcess.GetProcessState(db, configurationName);
                    return stateAfterRestart.LastLsn > 0;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(resumed, "Expected CDC process to resume");

                // insert a second row
                ulong lsnBeforeSecond = stateAfterRestart.LastLsn;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('SecondInsert')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeSecond);

                bool secondArrived = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments == initialCount + 2;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(secondArrived, "Expected second insert to arrive");

                // verify no duplicates: total should be exactly initialCount + 2
                var finalStats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(initialCount + 2, finalStats.CountOfDocuments);
            }
        }
    }
}

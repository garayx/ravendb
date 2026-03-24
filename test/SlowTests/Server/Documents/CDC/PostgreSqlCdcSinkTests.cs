using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.CDC;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
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
                        Patch = x.Patch // still doesnt work, but there is code for it :)
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

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
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

                // verify initial load worked by inserting a row before disable
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);
                ulong lsnBeforePreDisable = state.LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('BeforeDisable')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforePreDisable);

                bool preDisableArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "BeforeDisable");
                }, true, timeout: 60_000, interval: 1000);
                Assert.True(preDisableArrived, "Expected pre-disable insert to arrive");

                DatabaseStatistics statsBeforeDisable = store.Maintenance.Send(new GetStatisticsOperation());

                // disable the database
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                await Task.Delay(5000);

                // insert while disabled � this should be picked up after re-enable via the persisted slot
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('WhileDisabled')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                // re-enable the database
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));

                // re-acquire the database instance (it was recreated after enable)
                db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // wait for the CDC process to actually be running
                bool cdcRunning = await WaitForValueAsync(() =>
                {
                    return db.CdcSinkLoader?.Processes?.Length > 0;
                }, true, timeout: 60_000, interval: 1000);
                Assert.True(cdcRunning, "Expected CDC process to be running after re-enable");

                // the row inserted while disabled should arrive through the replication slot
                bool disabledInsertArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "WhileDisabled");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(disabledInsertArrived, "Expected the row inserted while database was disabled to arrive via CDC");

                // also insert a row after re-enable
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('AfterEnable')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool afterEnableArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "AfterEnable");
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(afterEnableArrived, "Expected a new document to arrive after database disable/enable cycle");
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

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
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
                await Task.Delay(5000);

                // insert while disabled — the slot retains this
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('SecondInsert')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // wait for CDC process to be running
                bool cdcRunning = await WaitForValueAsync(() =>
                {
                    return db.CdcSinkLoader?.Processes?.Length > 0;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(cdcRunning, "Expected CDC process to resume");

                bool secondArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "SecondInsert");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(secondArrived, "Expected second insert to arrive");

                // verify no duplicates by checking that each customer name appears at most once
                using (var session = store.OpenSession())
                {
                    var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    var firstInsertCount = allCustomers.Count(c => c.Firstname == "FirstInsert");
                    var secondInsertCount = allCustomers.Count(c => c.Firstname == "SecondInsert");
                    Assert.Equal(1, firstInsertCount);
                    Assert.Equal(1, secondInsertCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateDeleteFromPostgreSQL(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_delete_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert a fresh customer with no FK references so we can freely delete it
                ulong lsnBeforeInsert = state.LastLsn;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('WillBeDeleted')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInsert);

                bool inserted = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                        return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "WillBeDeleted");
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(inserted, "Expected 'WillBeDeleted' customer to arrive before the delete");

                string insertedId;
                using (var session = store.OpenSession())
                {
                    var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    insertedId = session.Advanced.GetDocumentId(all.First(c => c.Firstname == "WillBeDeleted"));
                }

                DatabaseStatistics statsBeforeDelete = store.Maintenance.Send(new GetStatisticsOperation());
                ulong lsnBeforeDelete = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                var pgId = insertedId.Split('/')[1];
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {pgId}";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeDelete);

                bool deleted = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments == statsBeforeDelete.CountOfDocuments - 1;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(deleted, $"Expected {insertedId} to be deleted in RavenDB after DELETE in PostgreSQL");

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Customer>(insertedId));
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateMultipleDeletesInSingleTransaction(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_multi_delete_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert 3 fresh customers with no FK references so we can freely delete them
                ulong lsnBeforeInserts = state.LastLsn;
                var insertedNames = new[] { "DeleteMe1", "DeleteMe2", "DeleteMe3" };
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);
                    foreach (var name in insertedNames)
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.Transaction = tx;
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('{name}')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInserts);

                bool allInserted = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                        return insertedNames.All(n => all.Any(c => c.Firstname == n));
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(allInserted, "Expected all 3 inserted customers to arrive");

                List<string> insertedIds;
                using (var session = store.OpenSession())
                {
                    var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    insertedIds = insertedNames
                        .Select(n => session.Advanced.GetDocumentId(all.First(c => c.Firstname == n)))
                        .ToList();
                }

                DatabaseStatistics statsBeforeDelete = store.Maintenance.Send(new GetStatisticsOperation());
                ulong lsnBeforeDelete = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);
                    foreach (var docId in insertedIds)
                    {
                        var pgId = docId.Split('/')[1];
                        using var cmd = conn.CreateCommand();
                        cmd.Transaction = tx;
                        cmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {pgId}";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeDelete);

                bool allDeleted = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments == statsBeforeDelete.CountOfDocuments - 3;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(allDeleted, "Expected all 3 customers deleted in the transaction to be removed from RavenDB");

                using (var session = store.OpenSession())
                {
                    foreach (var docId in insertedIds)
                        Assert.Null(session.Load<Customer>(docId));
                }
            }
        }


        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateDeleteAndInsertInSameTransaction(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_delete_insert_tx_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert a fresh customer first so we have one with no FK references to delete
                ulong lsnBeforeInsert = state.LastLsn;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('ToDeleteInTx')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeInsert);

                bool inserted = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                        return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "ToDeleteInTx");
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(inserted, "Expected 'ToDeleteInTx' to arrive before the delete+insert transaction");

                string toDeleteId;
                using (var session = store.OpenSession())
                {
                    var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    toDeleteId = session.Advanced.GetDocumentId(all.First(c => c.Firstname == "ToDeleteInTx"));
                }

                // advance sequence again for the upcoming insert
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                DatabaseStatistics statsBeforeTx = store.Maintenance.Send(new GetStatisticsOperation());
                ulong lsnBeforeTx = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                // delete the fresh customer and insert another one in the same transaction
                var pgId = toDeleteId.Split('/')[1];
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var deleteCmd = conn.CreateCommand())
                    {
                        deleteCmd.Transaction = tx;
                        deleteCmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {pgId}";
                        await deleteCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.Transaction = tx;
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('NewAfterDelete')";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeTx);

                // net effect: count stays the same (-1 delete +1 insert)
                bool countUnchanged = await WaitForValueAsync(() =>
                {
                    var currentStats = store.Maintenance.Send(new GetStatisticsOperation());
                    return currentStats.CountOfDocuments == statsBeforeTx.CountOfDocuments;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(countUnchanged, "Expected document count to remain the same after one delete and one insert");

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Customer>(toDeleteId));
                    var all = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                    Assert.NotNull(all.FirstOrDefault(c => c.Firstname == "NewAfterDelete"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateDeleteAndInsertInSameTransaction2(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_delete_insert_tx_test2";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert a fresh customer with no FK references so we can safely delete+reinsert it
                ulong lsnBeforeSetup = state.LastLsn;
                int freshId;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('OrigName') RETURNING ""id""";
                    freshId = (int)await cmd.ExecuteScalarAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeSetup);
                Assert.True(await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Load<Customer>($"Customer/{freshId}")?.Firstname == "OrigName";
                }, true, timeout: 60_000, interval: 1000), "Expected fresh customer to arrive");

                ulong lsnBeforeTx = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                // delete + insert same id in a single transaction
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var deleteCmd = conn.CreateCommand())
                    {
                        deleteCmd.Transaction = tx;
                        deleteCmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {freshId}";
                        await deleteCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.Transaction = tx;
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""id"", ""firstname"") VALUES ({freshId}, 'NewName')";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeTx);

                bool updated = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        return session.Load<Customer>($"Customer/{freshId}")?.Firstname == "NewName";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updated, $"Expected Customer/{freshId} to exist with Firstname='NewName' after delete+insert of same id in single transaction");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateUpdateWithReplicaIdentityFull(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                // set REPLICA IDENTITY FULL so PostgreSQL sends FullUpdateMessage
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"ALTER TABLE ""{schemaName}"".""customer"" REPLICA IDENTITY FULL";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                string configurationName = "cdc_full_update_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.NotEqual("FullUpdateName", customer.Firstname);
                }

                ulong lsnBeforeUpdate = state.LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'FullUpdateName' WHERE ""id"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeUpdate);

                bool updated = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var customer = session.Load<Customer>("Customer/1");
                        return customer?.Firstname == "FullUpdateName";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updated, "Expected the customer document to be updated via CDC with REPLICA IDENTITY FULL");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanRecoverAfterPostgreSqlConnectionDropped(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_connection_drop_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                // forcefully terminate the replication backend in PostgreSQL
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT pg_terminate_backend(active_pid)
                        FROM pg_replication_slots
                        WHERE active_pid IS NOT NULL AND slot_type = 'logical'";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                // give the CDC process time to detect the broken connection and recreate the consumer
                await Task.Delay(10_000, cts.Token);

                // insert a new row — it should arrive once the process reconnects
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('AfterDrop')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                bool arrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == "AfterDrop");
                }, true, timeout: 120_000, interval: 1000);

                Assert.True(arrived, "Expected document to arrive after PostgreSQL connection was dropped and recovered");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CdcProcessStateSurvivesMultipleRestarts(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_multi_restart_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // perform 3 disable/enable cycles, inserting a row while disabled each time
                for (int i = 1; i <= 3; i++)
                {
                    store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                    await Task.Delay(3000);

                    await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);
                    using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                    {
                        await conn.OpenAsync(cts.Token);
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Restart{i}')";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                    db = await Databases.GetDocumentDatabaseInstanceFor(store);

                    bool cdcRunning = await WaitForValueAsync(() =>
                    {
                        return db.CdcSinkLoader?.Processes?.Length > 0;
                    }, true, timeout: 60_000, interval: 1000);
                    Assert.True(cdcRunning, $"Expected CDC process to be running after restart #{i}");

                    bool arrived = await WaitForValueAsync(() =>
                    {
                        using var session = store.OpenSession();
                        return session.Advanced.RawQuery<Customer>("from Customer").ToList().Any(c => c.Firstname == $"Restart{i}");
                    }, true, timeout: 120_000, interval: 1000);

                    Assert.True(arrived, $"Expected 'Restart{i}' to arrive after restart #{i}");

                    // verify the row is exactly once (no duplicates from slot replay)
                    using (var session = store.OpenSession())
                    {
                        var allCustomers = session.Advanced.RawQuery<Customer>("from Customer").ToList();
                        Assert.Equal(1, allCustomers.Count(c => c.Firstname == $"Restart{i}"));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateUpdateWithReplicaIdentityUsingIndex(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                // create a unique index on the PK column and set REPLICA IDENTITY USING INDEX
                // so PostgreSQL sends IndexUpdateMessage
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"CREATE UNIQUE INDEX customer_id_idx ON ""{schemaName}"".""customer"" (""id"")";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"ALTER TABLE ""{schemaName}"".""customer"" REPLICA IDENTITY USING INDEX customer_id_idx";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                }

                string configurationName = "cdc_index_update_test";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.NotEqual("IndexUpdateName", customer.Firstname);
                }

                ulong lsnBeforeUpdate = state.LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'IndexUpdateName' WHERE ""id"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeUpdate);

                bool updated = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var customer = session.Load<Customer>("Customer/1");
                        return customer?.Firstname == "IndexUpdateName";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updated, "Expected the customer document to be updated via CDC with REPLICA IDENTITY USING INDEX");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task NestedCollection_InitialLoadEmbedsChildRows(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                // Insert categories and productcategory rows
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
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (2, 1)";
                        await cmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (3, 2)";
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

                string configurationName = "cdc_nested_initial_load";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 2);

                // Category/1 should have 2 productcategory items (product 1 and 2)
                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.NotNull(cat1);
                    Assert.Equal("Beverages", cat1.Name);
                    Assert.NotNull(cat1.Productcategory);
                    Assert.Equal(2, cat1.Productcategory.Length);
                    Assert.Contains(cat1.Productcategory, p => p.Productid == 1);
                    Assert.Contains(cat1.Productcategory, p => p.Productid == 2);

                    // Category/2 should have 1 productcategory item (product 3)
                    var cat2 = session.Load<CategoryWithNested>("Category/2");
                    Assert.NotNull(cat2);
                    Assert.NotNull(cat2.Productcategory);
                    Assert.Single(cat2.Productcategory);
                    Assert.Equal(3, cat2.Productcategory[0].Productid);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task NestedCollection_InsertReplicatesAsNestedProperty(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                // Insert categories (no productcategory rows yet)
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (1, 'Beverages')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (2, 'Condiments')";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
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

                string configurationName = "cdc_nested_insert";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 2);

                // No productcategory rows exist yet after initial load (none in insert.sql)
                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.NotNull(cat1);
                    Assert.NotNull(cat1.Productcategory);
                    Assert.Empty(cat1.Productcategory);
                }

                ulong lsnBefore = state.LastLsn;

                // Insert a productcategory row via CDC
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (1, 1)";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBefore);

                // Category/1 should now have a Productcategory array with one item
                bool nestedArrived = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    return cat1?.Productcategory != null && cat1.Productcategory.Length == 1;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(nestedArrived, "Expected productcategory to be nested inside Category/1 after CDC insert");

                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.Equal(1, cat1.Productcategory[0].Productid);
                    Assert.Equal(1, cat1.Productcategory[0].Categoryid);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task NestedCollection_DeleteRemovesFromNestedArray(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
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
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (2, 1)";
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

                string configurationName = "cdc_nested_delete";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 2);

                // Verify initial state: Category/1 has 2 nested items
                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.NotNull(cat1);
                    Assert.Equal(2, cat1.Productcategory.Length);
                }

                ulong lsnBefore = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                // Delete one productcategory row
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"DELETE FROM ""{schemaName}"".""productcategory"" WHERE ""productid"" = 1 AND ""categoryid"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBefore);

                // Category/1 should now have only 1 nested item
                bool nestedDeleted = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    return cat1?.Productcategory != null && cat1.Productcategory.Length == 1;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(nestedDeleted, "Expected productcategory item to be removed from Category/1 after CDC delete");

                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.Equal(2, cat1.Productcategory[0].Productid);
                    Assert.Equal(1, cat1.Productcategory[0].Categoryid);
                }
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CanReplicateDeleteAndPutOfSameIdInSingleBatch(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_delete_put_same_id";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);
                await AdvanceCustomerSequence(connectionString, schemaName, cts.Token);

                // insert a fresh customer with no FK references so we can safely delete+reinsert it
                ulong lsnBeforeSetup = state.LastLsn;
                int freshId;
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""firstname"") VALUES ('Original') RETURNING ""id""";
                    freshId = (int)await cmd.ExecuteScalarAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeSetup);
                Assert.True(await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Load<Customer>($"Customer/{freshId}")?.Firstname == "Original";
                }, true, timeout: 60_000, interval: 1000), "Expected fresh customer to arrive");

                ulong lsnBefore = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var deleteCmd = conn.CreateCommand())
                    {
                        deleteCmd.Transaction = tx;
                        deleteCmd.CommandText = $@"DELETE FROM ""{schemaName}"".""customer"" WHERE ""id"" = {freshId}";
                        await deleteCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.Transaction = tx;
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""customer"" (""id"", ""firstname"") VALUES ({freshId}, 'Resurrected')";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBefore);

                bool updated = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    return session.Load<Customer>($"Customer/{freshId}")?.Firstname == "Resurrected";
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(updated, $"Expected Customer/{freshId} to exist with Firstname='Resurrected' after delete+put of same id in single transaction");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task NestedCollection_DeleteAndPutOfSameNestedIdInSingleBatch(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                // Pre-insert category and one productcategory row
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using (var cmd = conn.CreateCommand())
                    { cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (1, 'Beverages')"; await cmd.ExecuteNonQueryAsync(cts.Token); }
                    using (var cmd = conn.CreateCommand())
                    { cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""category"" (""id"", ""name"") VALUES (2, 'Condiments')"; await cmd.ExecuteNonQueryAsync(cts.Token); }
                    using (var cmd = conn.CreateCommand())
                    { cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (1, 1)"; await cmd.ExecuteNonQueryAsync(cts.Token); }
                    using (var cmd = conn.CreateCommand())
                    { cmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (2, 1)"; await cmd.ExecuteNonQueryAsync(cts.Token); }
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

                string configurationName = "cdc_nested_delete_put";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName,
                    collections: collections, expectedMinDocuments: 2);

                using (var session = store.OpenSession())
                {
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    Assert.NotNull(cat1);
                    Assert.Equal(2, cat1.Productcategory.Length);
                }

                ulong lsnBefore = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    await using var tx = await conn.BeginTransactionAsync(cts.Token);

                    using (var deleteCmd = conn.CreateCommand())
                    {
                        deleteCmd.Transaction = tx;
                        deleteCmd.CommandText = $@"DELETE FROM ""{schemaName}"".""productcategory"" WHERE ""productid"" = 1 AND ""categoryid"" = 1";
                        await deleteCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    using (var insertCmd = conn.CreateCommand())
                    {
                        insertCmd.Transaction = tx;
                        insertCmd.CommandText = $@"INSERT INTO ""{schemaName}"".""productcategory"" (""productid"", ""categoryid"") VALUES (3, 1)";
                        await insertCmd.ExecuteNonQueryAsync(cts.Token);
                    }

                    await tx.CommitAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBefore);

                // Category/1 should now have 2 nested items: productid=2 (kept) and productid=3 (new)
                bool nestedUpdated = await WaitForValueAsync(() =>
                {
                    using var session = store.OpenSession();
                    var cat1 = session.Load<CategoryWithNested>("Category/1");
                    if (cat1?.Productcategory == null || cat1.Productcategory.Length != 2)
                        return false;
                    var ids = cat1.Productcategory.Select(p => p.Productid).OrderBy(x => x).ToArray();
                    return ids[0] == 2 && ids[1] == 3;
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(nestedUpdated, "Expected Category/1 to have nested items with Productid=2 and Productid=3 after delete+put of same nested id in single transaction");
            }
        }

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cdc, NpgSqlRequired = true)]
        [RequiresNpgSqlInlineData]
        public async Task CdcUpdateRecreatesDocumentDeletedLocally(MigrationProvider provider)
        {
            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: "northwind", includeData: true))
            {
                string configurationName = "cdc_update_after_local_delete";
                var (state, _) = await SetupAndWaitForInitialLoad(store, db, connectionString, schemaName, configurationName);

                // Step 1: Verify the document arrived via initial load
                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("Customer/1");
                    Assert.NotNull(customer);
                    Assert.Equal("Maria", customer.Firstname);
                }

                // Step 2: Delete the document locally via RavenDB session
                using (var session = store.OpenSession())
                {
                    session.Delete("Customer/1");
                    session.SaveChanges();
                }

                // Verify the document is gone
                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Customer>("Customer/1"));
                }

                ulong lsnBeforeUpdate = CdcSinkProcess.GetProcessState(db, configurationName).LastLsn;

                // Step 3: Update the same row in PostgreSQL — this should recreate the document in RavenDB
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    await conn.OpenAsync(cts.Token);
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"UPDATE ""{schemaName}"".""customer"" SET ""firstname"" = 'RecreatedViaCdc' WHERE ""id"" = 1";
                    await cmd.ExecuteNonQueryAsync(cts.Token);
                }

                await WaitForLsnAdvance(db, configurationName, lsnBeforeUpdate);

                // The CDC update should recreate the document with the new value
                bool recreated = await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var customer = session.Load<Customer>("Customer/1");
                        return customer?.Firstname == "RecreatedViaCdc";
                    }
                }, true, timeout: 60_000, interval: 1000);

                Assert.True(recreated, "Expected Customer/1 to be recreated with Firstname='RecreatedViaCdc' after CDC update on a locally-deleted document");
            }
        }
    }
}

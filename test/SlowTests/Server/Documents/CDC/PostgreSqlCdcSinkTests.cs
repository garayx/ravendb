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

        [RavenTheory(RavenTestCategory.PostgreSql | RavenTestCategory.Cnc, NpgSqlRequired = true)]
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

        //private readonly HashSet<string> _definedTopics = new HashSet<string>();


        //protected CdcSinkConfiguration SetupPostgreSqlCdcSink(DocumentStore store, string script, List<string> queues,
        //    string configurationName = null,
        //    string transformationName = null, Dictionary<string, string> configuration = null,
        //    string bootstrapServers = null)
        //{
        //    var connectionStringName = $"{store.Database} to Kafka";

        //    CdcSinkScript CdcSinkScript = new CdcSinkScript
        //    {
        //        Name = transformationName ?? $"Queue Sink : {connectionStringName}",
        //        Queues = new List<string>(queues),
        //        Script = script,
        //    };
        //    var config = new CdcSinkConfiguration
        //    {
        //        Name = configurationName ?? connectionStringName,
        //        ConnectionStringName = connectionStringName,
        //        Scripts = { CdcSinkScript },
        //        BrokerType = CdcBrokerType.PostgreSQL
        //    };

        //    foreach (var queue in queues)
        //    {
        //        _definedTopics.Add(queue);
        //    }

        //    //AddCdcSink(store, config,
        //    //    new CdcConnectionString
        //    //    {
        //    //        Name = connectionStringName,
        //    //        BrokerType = CdcBrokerType.PostgreSQL,
        //    //        PostgresqlConnectionSettings = new PostgresqlConnectionSettings()                    {
        //    //            ConnectionOptions = configuration,
        //    //            BootstrapServers = bootstrapServers ?? KafkaConnectionString.Instance.VerifiedUrl.Value,
        //    //        }
        //    //    });

        //    return config;
        //}

        //public static IProducer<string, byte[]> CreateKafkaProducer(string bootstrapServers = null)
        //{
        //    ProducerConfig config = new()
        //    {
        //        BootstrapServers = bootstrapServers ?? KafkaConnectionString.Instance.VerifiedUrl.Value,
        //        EnableIdempotence = true
        //    };

        //    IProducer<string, byte[]> producer = new ProducerBuilder<string, byte[]>(config).Build();
        //    return producer;
        //}

        //private void CleanupTopics()
        //{
        //    if (_definedTopics.Count == 0 || RequiresKafkaRetryFactAttribute.CanConnect == false)
        //        return;

        //    var config = new AdminClientConfig { BootstrapServers = KafkaConnectionString.Instance.VerifiedUrl.Value };
        //    var adminClient = new AdminClientBuilder(config).Build();

        //    try
        //    {
        //        adminClient.DeleteTopicsAsync(_definedTopics).Wait();
        //    }
        //    catch (Exception e)
        //    {
        //        if (e.InnerException is DeleteTopicsException deleteEx)
        //        {
        //            if (deleteEx.Results.All(x => x.Error.Code == ErrorCode.UnknownTopicOrPart)) // topic does not exist
        //                return;
        //        }

        //        throw new InvalidOperationException($"Failed to cleanup topics: {string.Join(", ", _definedTopics)}. Check inner exceptions for details", e);
        //    }
        //}

        //public override void Dispose()
        //{
        //    base.Dispose();
        //    CleanupTopics();
        //}
    }
}

﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using SlowTests.Smuggler;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ShardedSmugglerTests : RavenTestBase
    {
        public ShardedSmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {

        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }

        private async Task InsertData(IDocumentStore store1, string[] names, RavenServer server)
        {
            using (var session = store1.OpenAsyncSession())
            {
                await RevisionsHelper.SetupRevisionsAsync(store1, configuration: new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                });

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                //Time series
                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                //counters
                session.CountersFor("users/2").Increment("Downloads", 100);
                //Attachments
                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }

            //tombstone + revision
            using (var session = store1.OpenAsyncSession())
            {
                session.Delete("users/4");
                var user = await session.LoadAsync<User>("users/1");
                user.Age = 10;
                await session.SaveChangesAsync();
            }

            //subscription
            await store1.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            //Identity
            var result1 = store1.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

            //CompareExchange
            var user1 = new User
            {
                Name = "Toli"
            };
            var user2 = new User
            {
                Name = "Mitzi"
            };

            var operationResult = await store1.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/toli", user1, 0));
            operationResult = await store1.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/mitzi", user2, 0));
            var result = await store1.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("cat/mitzi", operationResult.Index));

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Efrat, DevelopmentHelper.Severity.Normal, "need to wait for sharding cluster transaction issue - RavenDB-13111");
            //Cluster transaction
            // using var session2 = store1.OpenAsyncSession(new SessionOptions
            // {
            //     TransactionMode = TransactionMode.ClusterWide
            // });
            //
            // var user4 = new User { Name = "Ayende" };
            // await session2.StoreAsync(user4);
            // await session2.StoreAsync(new { ReservedFor = user4.Id }, "usernames/" + user4.Name);
            //
            // await session2.SaveChangesAsync();

            //Index
            await new Index().ExecuteAsync(store1);
        }

        private async Task CheckData(DocumentStore store2, string[] names)
        {
            var db = await GetDocumentDatabaseInstanceFor(store2, store2.Database);
            //doc
            Assert.Equal(3, db.DocumentsStorage.GetNumberOfDocuments());
            //Assert.Equal(1, detailedStats.CountOfCompareExchangeTombstones); //TODO - Not working for 4.2
            //tombstone
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(1, db.DocumentsStorage.GetNumberOfTombstones(context));
                Assert.Equal(16, db.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context));
            }

            //Index
            var indexes = await store2.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
            Assert.Equal(1, indexes.Length);

            //Subscriptions
            var subscriptionDocuments = await store2.Subscriptions.GetSubscriptionsAsync(0, 10);
            Assert.Equal(1, subscriptionDocuments.Count);

            using (var session = store2.OpenSession())
            {
                //Time series
                var val = session.TimeSeriesFor("users/1", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/3", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                //Counters
                var counterValue = session.CountersFor("users/2").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store2.OpenAsyncSession())
            {
                for (var i = 0; i < names.Length; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i + 1));
                    var metadata = session.Advanced.GetMetadataFor(user);

                    //Attachment
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (i == 0)
                    {
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 1)
                    {
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 2)
                    {
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }

                await session.StoreAsync(new User() { Name = "Toli" }, "users|");
                await session.SaveChangesAsync();
            }
            //Identity
            using (var session = store2.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1991");
                Assert.NotNull(user);


            }
            //CompareExchange
            using (var session = store2.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("cat/toli"));
                Assert.NotNull(user1);
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Efrat, DevelopmentHelper.Severity.Normal, "need to wait for sharding cluster transaction issue - RavenDB-13111");

                //TODO - need to wait for sharding cluster transaction issue - RavenDB-13111
                // user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/usernames/ayende"));
                // Assert.NotNull(user1);
                //
                // user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/users/1-a"));
                // Assert.NotNull(user1);
            }

        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ManyDocsRegularToShardToRegular()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    using (var session = store1.BulkInsert())
                    {
                        for (int i = 0; i < 12345; i++)
                        {
                            await session.StoreAsync(new User()
                            {
                                Name = "user/" + i
                            }, "users/" + i);
                        }
                    }
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                OperateOnTypes = DatabaseItemType.Documents
                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                            WaitForUserToContinueTheTest(store3);
                            var detailedStats = store3.Maintenance.Send(new GetDetailedStatisticsOperation());
                            //doc
                            Assert.Equal(12345, detailedStats.CountOfDocuments);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }


        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RegularToShardToRegular()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    await InsertData(store1, names, Server);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions


                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions


                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                OperateOnTypes = DatabaseItemType.Documents
                                                 | DatabaseItemType.TimeSeries
                                                 | DatabaseItemType.CounterGroups
                                                 | DatabaseItemType.Attachments
                                                 | DatabaseItemType.Tombstones
                                                 | DatabaseItemType.DatabaseRecord
                                                 | DatabaseItemType.Subscriptions
                                                 | DatabaseItemType.Identities
                                                 | DatabaseItemType.CompareExchange
                                                 | DatabaseItemType.CompareExchangeTombstones // todo check test after fix
                                                 | DatabaseItemType.RevisionDocuments
                                                 | DatabaseItemType.Indexes
                                                 | DatabaseItemType.LegacyAttachments // todo test
                                                 | DatabaseItemType.LegacyAttachmentDeletions // todo test
                                                 | DatabaseItemType.LegacyDocumentDeletions //todo test


                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));
                            WaitForUserToContinueTheTest(store3);
                            await CheckData(store3, names);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanImportLegacyCountersToShard()
        {
            var assembly = typeof(SmugglerApiTests).Assembly;
            var file = GetTempFileName();
            using (var fs = assembly.GetManifestResourceStream("SlowTests.Data.legacy-counters.4.1.5.ravendbdump"))
            using (var store = Sharding.GetDocumentStore())
            {
                var options = new DatabaseSmugglerImportOptions();


#pragma warning disable 618
                options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Counters;
#pragma warning restore 618

                var operation = await store.Smuggler.ImportAsync(options, fs);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions() { OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.CounterGroups }, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var store2 = GetDocumentStore())
                {
                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions() { OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.CounterGroups }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(29, stats.CountOfCounterEntries);

                    using (var session = store2.OpenSession())
                    {
                        var q = session.Query<Supplier>().ToList();
                        Assert.Equal(29, q.Count);

                        foreach (var supplier in q)
                        {
                            var counters = session.CountersFor(supplier).GetAll();
                            Assert.Equal(1, counters.Count);
                            Assert.Equal(10, counters["likes"]);
                        }
                    }
                }
            }
        }

        [Fact(Skip = "For testing")]
        public async Task RegularToRegular()
        {
            var file = GetTempFileName();

            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    await InsertData(store1, names, Server);
                    WaitForUserToContinueTheTest(store1);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.Indexes
                        // | DatabaseItemType.CompareExchangeTombstones

                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.Indexes
                            // | DatabaseItemType.CompareExchangeTombstones

                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                        WaitForUserToContinueTheTest(store2);
                        await CheckData(store2, names);



                    }

                }
            }
            finally
            {
                File.Delete(file);

            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanExportAndImportDatabaseRecordToShardDatabase()
        {
            var file = Path.GetTempFileName();
            var file2 = Path.GetTempFileName();
            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            string privateKey;
            using (var pullReplicationCertificate =
                new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable))
            {
                privateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Pfx));
            }
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1",

                    ModifyDatabaseRecord = record =>
                    {
                        record.ConflictSolverConfig = new ConflictSolver
                        {
                            ResolveToLatest = false,
                            ResolveByCollection = new Dictionary<string, ScriptResolver>
                                {
                                    {
                                        "ConflictSolver", new ScriptResolver()
                                        {
                                            Script = "Script"
                                        }
                                    }
                                }
                        };
                        record.Sorters = new Dictionary<string, SorterDefinition>
                        {
                            {
                                "MySorter", new SorterDefinition
                                {
                                    Name = "MySorter",
                                    Code = GetCode("RavenDB_8355.MySorter.cs")
                                }
                            }
                        };
                        record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                        {
                            {
                                "MyAnalyzer", new AnalyzerDefinition
                                {
                                    Name = "MyAnalyzer",
                                    Code = GetCode("RavenDB_14939.MyAnalyzer.cs")
                                }
                            }
                        };
                    }
                }))
                using (var store2 = GetDocumentStore())
                using (var store3 = Sharding.GetDocumentStore())
                {
                    // WaitForUserToContinueTheTest(store1);
                    var config = Backup.CreateBackupConfiguration(backupPath: "FolderPath", fullBackupFrequency: "0 */1 * * *", incrementalBackupFrequency: "0 */6 * * *", mentorNode: "A", name: "Backup");

                    store1.Maintenance.Send(new UpdateExternalReplicationOperation(new ExternalReplication("tempDatabase", "ExternalReplication")
                    {
                        TaskId = 1,
                        Name = "External",
                        DelayReplicationFor = new TimeSpan(4),
                        Url = "http://127.0.0.1/",
                        Disabled = false
                    }));
                    store1.Maintenance.Send(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink()
                    {
                        Database = "sinkDatabase",
                        CertificatePassword = (string)null,
                        CertificateWithPrivateKey = privateKey,
                        TaskId = 2,
                        Name = "Sink",
                        HubName = "hub",
                        ConnectionStringName = "ConnectionName"
                    }));
                    store1.Maintenance.Send(new PutPullReplicationAsHubOperation(new PullReplicationDefinition()
                    {
                        TaskId = 3,
                        Name = "hub",
                        MentorNode = "A",
                        DelayReplicationFor = new TimeSpan(3),
                    }));

                    var result1 = store1.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                    {
                        Name = "ConnectionName",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    }));
                    Assert.NotNull(result1.RaftCommandIndex);

                    var sqlConnectionString = new SqlConnectionString
                    {
                        Name = "connection",
                        ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store1.Database};",
                        FactoryName = "System.Data.SqlClient"
                    };

                    var result2 = store1.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                    Assert.NotNull(result2.RaftCommandIndex);

                    store1.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration()
                    {
                        AllowEtlOnNonEncryptedChannel = true,
                        ConnectionStringName = "ConnectionName",
                        MentorNode = "A",
                        Name = "Etl",
                        TaskId = 4,
                        TestMode = true,
                        Transforms = {
                             new Transformation
                             {
                                 Name = $"ETL : 1",
                                 Collections = new List<string>(new[] {"Users"}),
                                 Script = null,
                                 ApplyToAllDocuments = false,
                                 Disabled = false
                             }
                         }
                    }));

                    store1.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(new SqlEtlConfiguration()
                    {
                        AllowEtlOnNonEncryptedChannel = true,
                        ForceQueryRecompile = false,
                        ConnectionStringName = "connection",
                        SqlTables =
                             {
                                 new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = false},
                                 new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = false},
                             },
                        Name = "sql",
                        ParameterizeDeletes = false,
                        MentorNode = "A",
                        Transforms = {
                             new Transformation
                             {
                                 Name = $"ETL : 2",
                                 Collections = new List<string>(new[] {"Users"}),
                                 Script = null,
                                 ApplyToAllDocuments = false,
                                 Disabled = false
                             }
                         }
                    }));
                    await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.DatabaseRecord
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    , file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store3.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    , file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    , file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    WaitForUserToContinueTheTest(store2);
                    var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store2)).PeriodicBackupRunner;
                    var backups = periodicBackupRunner.PeriodicBackups;

                    Assert.Equal("Backup", backups.First().Configuration.Name);
                    Assert.Equal(true, backups.First().Configuration.IncrementalBackupFrequency.Equals("0 */6 * * *"));
                    Assert.Equal(true, backups.First().Configuration.FullBackupFrequency.Equals("0 */1 * * *"));
                    Assert.Equal(BackupType.Backup, backups.First().Configuration.BackupType);
                    Assert.Equal(true, backups.First().Configuration.Disabled);

                    var record = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));

                    record.Settings.TryGetValue("Patching.MaxNumberOfCachedScripts", out string value);
                    Assert.Null(value);

                    Assert.NotNull(record.ConflictSolverConfig);
                    Assert.Equal(false, record.ConflictSolverConfig.ResolveToLatest);
                    Assert.Equal(1, record.ConflictSolverConfig.ResolveByCollection.Count);
                    Assert.Equal(true, record.ConflictSolverConfig.ResolveByCollection.TryGetValue("ConflictSolver", out ScriptResolver sr));
                    Assert.Equal("Script", sr.Script);

                    Assert.Equal(1, record.Sorters.Count);
                    Assert.Equal(true, record.Sorters.TryGetValue("MySorter", out SorterDefinition sd));
                    Assert.Equal("MySorter", sd.Name);
                    Assert.NotEmpty(sd.Code);

                    Assert.Equal(1, record.Analyzers.Count);
                    Assert.Equal(true, record.Analyzers.TryGetValue("MyAnalyzer", out AnalyzerDefinition ad));
                    Assert.Equal("MyAnalyzer", ad.Name);
                    Assert.NotEmpty(ad.Code);

                    Assert.Equal(1, record.ExternalReplications.Count);
                    Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                    Assert.Equal(true, record.ExternalReplications[0].Disabled);

                    Assert.Equal(1, record.SinkPullReplications.Count);
                    Assert.Equal("sinkDatabase", record.SinkPullReplications[0].Database);
                    Assert.Equal("hub", record.SinkPullReplications[0].HubName);
                    Assert.Equal((string)null, record.SinkPullReplications[0].CertificatePassword);
                    Assert.Equal(privateKey, record.SinkPullReplications[0].CertificateWithPrivateKey);
                    Assert.Equal(true, record.SinkPullReplications[0].Disabled);

                    Assert.Equal(1, record.HubPullReplications.Count);
                    Assert.Equal(new TimeSpan(3), record.HubPullReplications.First().DelayReplicationFor);
                    Assert.Equal("hub", record.HubPullReplications.First().Name);
                    Assert.Equal(true, record.HubPullReplications.First().Disabled);

                    Assert.Equal(1, record.RavenEtls.Count);
                    Assert.Equal("Etl", record.RavenEtls.First().Name);
                    Assert.Equal("ConnectionName", record.RavenEtls.First().ConnectionStringName);
                    Assert.Equal(true, record.RavenEtls.First().AllowEtlOnNonEncryptedChannel);
                    Assert.Equal(true, record.RavenEtls.First().Disabled);

                    Assert.Equal(1, record.SqlEtls.Count);
                    Assert.Equal("sql", record.SqlEtls.First().Name);
                    Assert.Equal(false, record.SqlEtls.First().ParameterizeDeletes);
                    Assert.Equal(false, record.SqlEtls.First().ForceQueryRecompile);
                    Assert.Equal("connection", record.SqlEtls.First().ConnectionStringName);
                    Assert.Equal(true, record.SqlEtls.First().AllowEtlOnNonEncryptedChannel);
                    Assert.Equal(true, record.SqlEtls.First().Disabled);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task PullReplicationCertificateExportAndImport()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            var hubSettings = new ConcurrentDictionary<string, string>();
            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates);
            var hubDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });

            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var pullReplicationCertificate = new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = hubCerts.ServerCertificate.Value,
                Server = hubServer,
                ModifyDatabaseName = _ => hubDB
            }))
            {
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullReplicationName,
                    new ReplicationHubAccess
                    {
                        Name = pullReplicationName,
                        CertificateBase64 = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert))
                    }));

                var operation = await hubStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.ReplicationHubCertificates
                                     | DatabaseItemType.DatabaseRecord

                }, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));


                using (var shardStore = Sharding.GetDocumentStore(new Options
                {
                    ClientCertificate = hubCerts.ServerCertificate.Value,
                    Server = hubServer,
                    ModifyDatabaseName = _ => hubDB + "shard"
                }))
                {
                    operation = await shardStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord

                    }, file);

                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await shardStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord
                    }, file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = GetDocumentStore(new Options
                    {
                        ClientCertificate = hubCerts.ServerCertificate.Value,
                        Server = hubServer,
                        ModifyDatabaseName = _ => hubDB + "shard2"
                    }))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord

                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        var accesses = await store2.Maintenance.SendAsync(new GetReplicationHubAccessOperation(pullReplicationName));
                        Assert.NotEmpty(accesses[0].Certificate);

                        //WaitForUserToContinueTheTest(store2, clientCert: hubCerts.ServerCertificate.Value);
                    }

                }

            }
        }
        private static string GetCode(string name)
        {
            using (var stream = GetDump(name))
            using (var reader = new StreamReader(stream))
                return reader.ReadToEnd();
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

        [Fact(Skip = "TODO")]
        public async Task RegularToShardToRegularEncrypted()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Efrat, DevelopmentHelper.Severity.Normal, "Handle RegularToShardToRegularEncrypted");

            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    await InsertData(store1, names, Server);
                    //WaitForUserToContinueTheTest(store1);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        OperateOnTypes = DatabaseItemType.Documents
                        // | DatabaseItemType.TimeSeries
                        // | DatabaseItemType.CounterGroups
                        // | DatabaseItemType.Attachments
                        // | DatabaseItemType.Tombstones
                        // | DatabaseItemType.DatabaseRecord
                        // | DatabaseItemType.Subscriptions
                        // | DatabaseItemType.Identities
                        // | DatabaseItemType.CompareExchange
                        //| DatabaseItemType.CompareExchangeTombstones
                        //| DatabaseItemType.RevisionDocuments
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); //TODO - EFRAT

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                            OperateOnTypes = DatabaseItemType.Documents
                            // | DatabaseItemType.TimeSeries
                            // | DatabaseItemType.CounterGroups
                            // | DatabaseItemType.Attachments
                            // | DatabaseItemType.Tombstones
                            // | DatabaseItemType.DatabaseRecord
                            // | DatabaseItemType.Subscriptions
                            // | DatabaseItemType.Identities
                            // | DatabaseItemType.CompareExchange
                            // | DatabaseItemType.CompareExchangeTombstones
                            //| DatabaseItemType.RevisionDocuments
                            //| DatabaseItemType.RevisionDocuments 

                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                            OperateOnTypes = DatabaseItemType.Documents
                            // | DatabaseItemType.TimeSeries
                            // | DatabaseItemType.CounterGroups
                            // | DatabaseItemType.Attachments
                            // | DatabaseItemType.Tombstones
                            // | DatabaseItemType.DatabaseRecord
                            // | DatabaseItemType.Subscriptions
                            // | DatabaseItemType.Identities
                            // | DatabaseItemType.CompareExchange
                            //| DatabaseItemType.CompareExchangeTombstones
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                                OperateOnTypes = DatabaseItemType.Documents
                                // | DatabaseItemType.TimeSeries
                                // | DatabaseItemType.CounterGroups
                                // | DatabaseItemType.Attachments
                                // | DatabaseItemType.Tombstones
                                // | DatabaseItemType.DatabaseRecord
                                // | DatabaseItemType.Subscriptions
                                // | DatabaseItemType.Identities
                                // | DatabaseItemType.CompareExchange
                                //| DatabaseItemType.CompareExchangeTombstones
                                //| DatabaseItemType.RevisionDocuments

                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                            WaitForUserToContinueTheTest(store3);
                            await CheckData(store3, names);
                        }
                    }

                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShardExportReturnState()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2",
                }))
                {
                    await InsertData(store1, names, Server);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions


                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions


                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions
                        }, file2);

                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        OperationState op = null;
                        WaitForValue(() =>
                        {
                            op = store2.Maintenance.Send(new GetOperationStateOperation(operation.Id));
                            return op.Status == OperationStatus.Completed;
                        }, true);
                        var res = (SmugglerResult)op.Result;
                        Assert.Equal(3, res.Documents.ReadCount);
                        Assert.Equal(1, res.Tombstones.ReadCount);
                        Assert.Equal(2, res.TimeSeries.ReadCount);
                        Assert.Equal(1, res.Counters.ReadCount);
                        Assert.Equal(3, res.Documents.Attachments.ReadCount);
                        Assert.Equal(15, res.RevisionDocuments.ReadCount);
                        Assert.Equal(1, res.Subscriptions.ReadCount);
                        Assert.Equal(1, res.Identities.ReadCount);
                        Assert.Equal(1, res.CompareExchange.ReadCount);
                        //Assert.Equal(0, res.CompareExchangeTombstones.ReadCount);
                        Assert.Equal(1, res.Indexes.ReadCount);
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }



    }
}

using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class OpenOldRawDataTests : RavenTestBase
    {
        public OpenOldRawDataTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Can_Open_v71_Data()
        {
            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(OpenOldRawDataTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            var zipPath = new PathSetting(fullBackupPath);
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, backupPath);
            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database) { Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" } }));

                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(17, stats.CountOfAttachments);
                Assert.Equal(16, stats.CountOfUniqueAttachments);
                Assert.Equal(17, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfTimeSeriesSegments);
            }
        }
    }
}

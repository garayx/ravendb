using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkDdlRequestValidationTests : RavenTestBase
    {
        public CdcSinkDdlRequestValidationTests(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly SqlConnectionString NpgsqlConnection = new()
        {
            FactoryName = "Npgsql",
            ConnectionString = "Host=ignored",
        };

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task RejectsInvalidSchemaIdentifier()
        {
            using var store = GetDocumentStore();

            var e = await Assert.ThrowsAsync<BadRequestException>(
                () => store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(NpgsqlConnection, new[] { "bad-schema" })));

            Assert.Contains("bad-schema", e.Message);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task RejectsEmptySchemaAndTableEntries()
        {
            using var store = GetDocumentStore();

            await Assert.ThrowsAsync<BadRequestException>(
                () => store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(NpgsqlConnection, new[] { "" })));

            await Assert.ThrowsAsync<BadRequestException>(
                () => store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(NpgsqlConnection, tables: new[] { "" })));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task RejectsUnsupportedProvider()
        {
            using var store = GetDocumentStore();

            var connection = new SqlConnectionString { FactoryName = "Oracle.ManagedDataAccess.Client", ConnectionString = "Data Source=ignored" };
            var e = await Assert.ThrowsAsync<BadRequestException>(
                () => store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(connection)));

            Assert.Contains("does not support provider", e.Message);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task RejectsUnknownConnectionStringName()
        {
            using var store = GetDocumentStore();

            var e = await Assert.ThrowsAsync<BadRequestException>(
                () => store.Maintenance.SendAsync(new GetCdcSinkDdlOperation("does-not-exist")));

            Assert.Contains("does-not-exist", e.Message);
        }
    }
}

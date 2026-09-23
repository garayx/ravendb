using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkMySqlDdlTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkMySqlDdlTests(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly string[] Fixture =
        {
            @"CREATE TABLE customers (
                id      INT AUTO_INCREMENT PRIMARY KEY,
                email   VARCHAR(200) NOT NULL,
                balance DECIMAL(12,2) NOT NULL DEFAULT 0,
                UNIQUE KEY uq_customers_email (email),
                CONSTRAINT ck_customers_balance CHECK (balance >= 0)
            )",
            @"CREATE TABLE orders (
                order_id        INT NOT NULL,
                line_no         INT NOT NULL,
                customer_id     INT NOT NULL,
                qty             INT NOT NULL,
                price           DECIMAL(10,2) NOT NULL,
                total           DECIMAL(12,2) AS (qty * price) STORED,
                parent_order_id INT NULL,
                parent_line_no  INT NULL,
                PRIMARY KEY (order_id, line_no),
                KEY ix_orders_customer (customer_id),
                CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                CONSTRAINT fk_orders_parent FOREIGN KEY (parent_order_id, parent_line_no) REFERENCES orders(order_id, line_no)
            )",
        };

        private void CreateFixture(string connectionString)
        {
            foreach (var statement in Fixture)
                ExecuteSqlQuery(MigrationProvider.MySQL_MySqlConnector, connectionString, statement);
        }

        private static SqlConnectionString Connection(string connectionString) => new()
        {
            FactoryName = "MySqlConnector.MySqlConnectorFactory",
            ConnectionString = connectionString,
        };

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ExportsShowCreateTableWithoutForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var databaseName, dataSet: null, includeData: false);
            CreateFixture(connectionString);

            using var store = GetDocumentStore();
            var ddl = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString)));

            Assert.NotNull(ddl);
            var files = ddl.GetFiles();
            Assert.Equal(
                new[] { $"{databaseName}/customers.sql", $"{databaseName}/orders.sql", CdcSinkDdlResult.ForeignKeysFileName }.OrderBy(k => k),
                files.Keys.OrderBy(k => k));

            var customers = files[$"{databaseName}/customers.sql"];
            Assert.StartsWith("CREATE TABLE `customers` (", customers);
            Assert.Contains("AUTO_INCREMENT", customers);
            Assert.Contains("UNIQUE KEY `uq_customers_email` (`email`)", customers);
            Assert.Contains("CONSTRAINT `ck_customers_balance` CHECK", customers);

            var orders = files[$"{databaseName}/orders.sql"];
            Assert.Contains("KEY `ix_orders_customer` (`customer_id`)", orders);
            Assert.DoesNotContain("FOREIGN KEY", orders);
            Assert.DoesNotContain(",\n)", orders);

            var foreignKeys = files[CdcSinkDdlResult.ForeignKeysFileName];
            Assert.Contains("ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE;", foreignKeys);
            Assert.Contains("ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_parent` FOREIGN KEY (`parent_order_id`, `parent_line_no`) REFERENCES `orders` (`order_id`, `line_no`);", foreignKeys);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ExportedDdlRecreatesAnIdenticalSchema()
        {
            using var sourceTeardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var sourceConnectionString, out _, dataSet: null, includeData: false);
            using var targetTeardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var targetConnectionString, out _, dataSet: null, includeData: false);
            CreateFixture(sourceConnectionString);

            using var store = GetDocumentStore();
            var source = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(sourceConnectionString)));

            ApplyDdlExport(MigrationProvider.MySQL_MySqlConnector, targetConnectionString, source);

            var target = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(targetConnectionString)));
            Assert.Equal(DdlFilesByTableName(source), DdlFilesByTableName(target));
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task HonorsTablesFilterAndMatchesDiscoveredTables()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var databaseName, dataSet: null, includeData: false);
            CreateFixture(connectionString);
            ExecuteSqlQuery(MigrationProvider.MySQL_MySqlConnector, connectionString, "CREATE VIEW customers_view AS SELECT id FROM customers");

            using var store = GetDocumentStore();

            var all = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString)));
            var schema = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(Connection(connectionString)));
            Assert.Equal(
                schema.Tables.Select(t => $"{t.SourceTableSchema}/{t.SourceTableName}.sql").OrderBy(x => x),
                all.GetFiles().Keys.Where(k => k != CdcSinkDdlResult.ForeignKeysFileName).OrderBy(x => x));

            var filtered = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), tables: new[] { "CUSTOMERS" }));
            Assert.Equal(new[] { $"{databaseName}/customers.sql" }, filtered.GetFiles().Keys.ToArray());

            Assert.Null(await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), tables: new[] { "missing" })));
        }
    }
}

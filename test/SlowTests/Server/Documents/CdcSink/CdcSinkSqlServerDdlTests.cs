using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkSqlServerDdlTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkSqlServerDdlTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string Fixture = @"
            CREATE TABLE dbo.customers (
                id       INT IDENTITY(1,1) NOT NULL CONSTRAINT pk_customers PRIMARY KEY CLUSTERED,
                email    NVARCHAR(200) NOT NULL CONSTRAINT uq_customers_email UNIQUE,
                name     VARCHAR(MAX) COLLATE Latin1_General_BIN2 NULL,
                balance  DECIMAL(12,2) NOT NULL CONSTRAINT df_customers_balance DEFAULT (0),
                created  DATETIME2(3) NULL,
                CONSTRAINT ck_customers_balance CHECK (balance >= 0)
            );

            CREATE TABLE dbo.orders (
                order_id        INT NOT NULL,
                line_no         INT NOT NULL,
                customer_id     INT NOT NULL,
                qty             INT NOT NULL,
                price           DECIMAL(10,2) NOT NULL,
                total           AS (qty * price) PERSISTED,
                parent_order_id INT NULL,
                parent_line_no  INT NULL,
                CONSTRAINT pk_orders PRIMARY KEY (order_id, line_no),
                CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES dbo.customers(id) ON DELETE CASCADE,
                CONSTRAINT fk_orders_parent FOREIGN KEY (parent_order_id, parent_line_no) REFERENCES dbo.orders(order_id, line_no)
            );

            CREATE INDEX ix_orders_customer ON dbo.orders (customer_id) INCLUDE (qty);
            CREATE INDEX ix_orders_big ON dbo.orders (price DESC) WHERE qty > 10;";

        private static SqlConnectionString Connection(string connectionString) => new()
        {
            FactoryName = "Microsoft.Data.SqlClient",
            ConnectionString = connectionString,
        };

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task ExportsTablesConstraintsIndexesAndForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, Fixture);

            using var store = GetDocumentStore();
            var ddl = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString)));

            Assert.NotNull(ddl);
            var files = ddl.GetFiles();
            Assert.Equal(new[] { "dbo/customers.sql", "dbo/orders.sql", CdcSinkDdlResult.ForeignKeysFileName }, files.Keys.OrderBy(k => k).ToArray());

            var customers = files["dbo/customers.sql"];
            Assert.Contains("CREATE TABLE [dbo].[customers] (", customers);
            Assert.Contains("[id] int IDENTITY(1, 1) NOT NULL", customers);
            Assert.Contains("[email] nvarchar(200) NOT NULL", customers);
            Assert.Contains("[name] varchar(max) COLLATE Latin1_General_BIN2 NULL", customers);
            Assert.Contains("[balance] decimal(12, 2) NOT NULL CONSTRAINT [df_customers_balance] DEFAULT ((0))", customers);
            Assert.Contains("[created] datetime2(3) NULL", customers);
            Assert.Contains("CONSTRAINT [pk_customers] PRIMARY KEY CLUSTERED ([id] ASC)", customers);
            Assert.Contains("CONSTRAINT [uq_customers_email] UNIQUE NONCLUSTERED ([email] ASC)", customers);
            Assert.Contains("CONSTRAINT [ck_customers_balance] CHECK ([balance]>=(0))", customers);

            var orders = files["dbo/orders.sql"];
            Assert.Contains("[total] AS ([qty]*[price]) PERSISTED", orders);
            Assert.Contains("CREATE NONCLUSTERED INDEX [ix_orders_customer] ON [dbo].[orders] ([customer_id] ASC) INCLUDE ([qty]);", orders);
            Assert.Contains("CREATE NONCLUSTERED INDEX [ix_orders_big] ON [dbo].[orders] ([price] DESC) WHERE ([qty]>(10));", orders);
            Assert.DoesNotContain("FOREIGN KEY", orders);

            var foreignKeys = files[CdcSinkDdlResult.ForeignKeysFileName];
            Assert.Contains("ALTER TABLE [dbo].[orders] ADD CONSTRAINT [fk_orders_customer] FOREIGN KEY ([customer_id]) REFERENCES [dbo].[customers] ([id]) ON DELETE CASCADE;", foreignKeys);
            Assert.Contains("ALTER TABLE [dbo].[orders] ADD CONSTRAINT [fk_orders_parent] FOREIGN KEY ([parent_order_id], [parent_line_no]) REFERENCES [dbo].[orders] ([order_id], [line_no]);", foreignKeys);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task ExportedDdlRecreatesAnIdenticalSchema()
        {
            using var sourceTeardown = WithSqlDatabase(MigrationProvider.MsSQL, out var sourceConnectionString, out _, dataSet: null, includeData: false);
            using var targetTeardown = WithSqlDatabase(MigrationProvider.MsSQL, out var targetConnectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.MsSQL, sourceConnectionString, Fixture);

            using var store = GetDocumentStore();
            var source = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(sourceConnectionString)));

            ApplyDdlExport(MigrationProvider.MsSQL, targetConnectionString, source);

            var target = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(targetConnectionString)));
            Assert.Equal(source.GetFiles(), target.GetFiles());
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task HonorsSchemasAndTablesFilters()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, Fixture);
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, "CREATE SCHEMA shop");
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, @"
                CREATE TABLE shop.products (id INT NOT NULL PRIMARY KEY, customer_id INT NULL REFERENCES dbo.customers(id));
                CREATE TABLE shop.categories (id INT NOT NULL PRIMARY KEY);");

            using var store = GetDocumentStore();

            var defaultSchema = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString)));
            Assert.DoesNotContain(defaultSchema.GetFiles().Keys, k => k.StartsWith("shop/"));

            var shop = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), new[] { "shop" }));
            var shopFiles = shop.GetFiles();
            Assert.Equal(new[] { "shop/categories.sql", "shop/products.sql", CdcSinkDdlResult.ForeignKeysFileName }, shopFiles.Keys.OrderBy(k => k).ToArray());
            Assert.Contains("REFERENCES [dbo].[customers] ([id])", shopFiles[CdcSinkDdlResult.ForeignKeysFileName]);

            var filtered = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), new[] { "dbo", "shop" }, new[] { "SHOP.categories", "customers" }));
            Assert.Equal(new[] { "dbo/customers.sql", "shop/categories.sql" }, filtered.GetFiles().Keys.OrderBy(k => k).ToArray());
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlCdcRequired = true)]
        public async Task ExcludesCdcCatalogTablesLikeDiscovery()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, "CREATE TABLE tracked (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100))");
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, "EXEC sys.sp_cdc_enable_db");
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, @"
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name   = N'tracked',
                    @role_name     = NULL");

            using var store = GetDocumentStore();
            var ddl = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), new[] { "dbo", "cdc" }));
            var schema = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(Connection(connectionString), new[] { "dbo", "cdc" }));

            var files = ddl.GetFiles().Keys.OrderBy(x => x).ToArray();
            Assert.Equal(new[] { "dbo/tracked.sql" }, files);
            Assert.Equal(schema.Tables.Select(t => $"{t.SourceTableSchema}/{t.SourceTableName}.sql").OrderBy(x => x), files);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task ReturnsNullWhenNoTablesMatch()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            using var store = GetDocumentStore();
            Assert.Null(await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString))));
        }
    }
}

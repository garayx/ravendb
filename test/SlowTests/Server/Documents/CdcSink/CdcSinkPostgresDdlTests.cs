using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkPostgresDdlTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresDdlTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string Fixture = @"
            CREATE TABLE customers (
                id          SERIAL PRIMARY KEY,
                email       VARCHAR(200) NOT NULL UNIQUE,
                name        TEXT COLLATE ""C"",
                balance     NUMERIC(12,2) DEFAULT 0 NOT NULL CHECK (balance >= 0),
                created_at  TIMESTAMPTZ DEFAULT now(),
                tags        TEXT[]
            );

            CREATE TABLE orders (
                order_id        BIGINT GENERATED ALWAYS AS IDENTITY,
                line_no         INTEGER NOT NULL,
                customer_id     INTEGER NOT NULL,
                qty             INTEGER NOT NULL,
                price           NUMERIC(10,2) NOT NULL,
                total           NUMERIC(12,2) GENERATED ALWAYS AS (qty * price) STORED,
                parent_order_id BIGINT,
                parent_line_no  INTEGER,
                PRIMARY KEY (order_id, line_no),
                CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                CONSTRAINT fk_orders_parent FOREIGN KEY (parent_order_id, parent_line_no) REFERENCES orders(order_id, line_no)
            );

            CREATE INDEX ix_orders_customer ON orders (customer_id);
            CREATE INDEX ix_orders_big ON orders (price) WHERE qty > 10;";

        private static SqlConnectionString Connection(string connectionString) => new()
        {
            FactoryName = "Npgsql",
            ConnectionString = connectionString,
        };

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ExportsTablesConstraintsIndexesAndForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, Fixture);

            using var store = GetDocumentStore();
            var ddl = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString)));

            Assert.NotNull(ddl);
            var files = ddl.GetFiles();
            Assert.Equal(new[] { "public/customers.sql", "public/orders.sql", CdcSinkDdlResult.ForeignKeysFileName }, files.Keys.OrderBy(k => k).ToArray());

            var customers = files["public/customers.sql"];
            Assert.Contains("CREATE TABLE \"public\".\"customers\"", customers);
            Assert.Contains("\"id\" serial", customers);
            Assert.Contains("\"email\" character varying(200) NOT NULL", customers);
            Assert.Contains("COLLATE pg_catalog.\"C\"", customers);
            Assert.Matches(@"""balance"" numeric\(12,2\) DEFAULT 0\S* NOT NULL", customers);
            Assert.Contains("\"tags\" text[]", customers);
            Assert.Contains("CONSTRAINT \"customers_pkey\" PRIMARY KEY (id)", customers);
            Assert.Contains("CONSTRAINT \"customers_email_key\" UNIQUE (email)", customers);
            Assert.Contains("CONSTRAINT \"customers_balance_check\" CHECK", customers);

            var orders = files["public/orders.sql"];
            Assert.Contains("\"order_id\" bigint GENERATED ALWAYS AS IDENTITY NOT NULL", orders);
            Assert.Matches(@"""total"" numeric\(12,2\) GENERATED ALWAYS AS \(.+\) STORED", orders);
            Assert.Contains("CREATE INDEX ix_orders_customer ON public.orders USING btree (customer_id);", orders);
            Assert.Contains("WHERE (qty > 10);", orders);
            Assert.DoesNotContain("FOREIGN KEY", orders);

            var foreignKeys = files[CdcSinkDdlResult.ForeignKeysFileName];
            Assert.Contains("ALTER TABLE \"public\".\"orders\" ADD CONSTRAINT \"fk_orders_customer\" FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE;", foreignKeys);
            Assert.Contains("ADD CONSTRAINT \"fk_orders_parent\" FOREIGN KEY (parent_order_id, parent_line_no) REFERENCES public.orders(order_id, line_no);", foreignKeys);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ExportedDdlRecreatesAnIdenticalSchema()
        {
            using var sourceTeardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var sourceConnectionString, out _, dataSet: null, includeData: false);
            using var targetTeardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var targetConnectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.NpgSQL, sourceConnectionString, Fixture);

            using var store = GetDocumentStore();
            var source = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(sourceConnectionString)));

            ApplyDdlExport(MigrationProvider.NpgSQL, targetConnectionString, source);

            var target = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(targetConnectionString)));
            Assert.Equal(source.GetFiles(), target.GetFiles());
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ExportsPartitionedTablesAndMatchesDiscoveredTables()
        {
            using var sourceTeardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var sourceConnectionString, out _, dataSet: null, includeData: false);
            using var targetTeardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var targetConnectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.NpgSQL, sourceConnectionString, @"
                CREATE TABLE events (
                    id         BIGINT NOT NULL,
                    created_on DATE NOT NULL,
                    payload    JSONB,
                    PRIMARY KEY (id, created_on)
                ) PARTITION BY RANGE (created_on);

                CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
                CREATE INDEX ix_events_payload ON events USING gin (payload);

                CREATE VIEW events_view AS SELECT id FROM events;");

            using var store = GetDocumentStore();
            var ddl = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(sourceConnectionString)));
            var schema = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(Connection(sourceConnectionString)));

            var files = ddl.GetFiles();
            Assert.Equal(
                schema.Tables.Select(t => $"{t.SourceTableSchema}/{t.SourceTableName}.sql").OrderBy(x => x),
                files.Keys.OrderBy(x => x));

            var parent = files["public/events.sql"];
            Assert.Contains("PARTITION BY RANGE (created_on)", parent);
            Assert.Contains("CREATE INDEX ix_events_payload ON public.events USING gin (payload);", parent);

            var partition = files["public/events_2026.sql"];
            Assert.Contains("CREATE TABLE \"public\".\"events_2026\" PARTITION OF \"public\".\"events\" FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');", partition);
            Assert.DoesNotContain("PRIMARY KEY", partition);
            Assert.DoesNotContain("CREATE INDEX", partition);

            ApplyDdlExport(MigrationProvider.NpgSQL, targetConnectionString, ddl);
            var target = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(targetConnectionString)));
            Assert.Equal(files, target.GetFiles());
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task HonorsSchemasAndTablesFilters()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, Fixture + @"
                CREATE SCHEMA shop;
                CREATE TABLE shop.products (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES public.customers(id));
                CREATE TABLE shop.categories (id INTEGER PRIMARY KEY);");

            using var store = GetDocumentStore();

            var shop = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), new[] { "shop" }));
            var shopFiles = shop.GetFiles();
            Assert.Equal(new[] { "shop/categories.sql", "shop/products.sql", CdcSinkDdlResult.ForeignKeysFileName }, shopFiles.Keys.OrderBy(k => k).ToArray());
            Assert.Contains("REFERENCES public.customers(id)", shopFiles[CdcSinkDdlResult.ForeignKeysFileName]);

            var filtered = await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), new[] { "public", "shop" }, new[] { "shop.categories", "customers" }));
            Assert.Equal(new[] { "public/customers.sql", "shop/categories.sql" }, filtered.GetFiles().Keys.OrderBy(k => k).ToArray());
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ReturnsNullWhenNoTablesMatch()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            using var store = GetDocumentStore();
            Assert.Null(await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString))));

            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, Fixture);
            Assert.Null(await store.Maintenance.SendAsync(new GetCdcSinkDdlOperation(Connection(connectionString), tables: new[] { "missing" })));
        }
    }
}

using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.AiHelper.Migration.Schema;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationSchemaCatalogTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Discovered_schema_indexes_columns_exactly()
    {
        var catalog = MigrationSamples.Catalog(("public", "orders", ["order_id", "ordered_at"]));

        Assert.True(catalog.Knows("orders"));
        Assert.True(catalog.Knows("public.orders"));
        Assert.True(catalog.HasColumn("orders", "order_id"));
        Assert.True(catalog.HasColumn("public.orders", "ORDER_ID"));
        Assert.False(catalog.HasColumn("orders", "nope"));
        Assert.Equal(["order_id", "ordered_at"], catalog.Columns("orders"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Unknown_table_is_not_known_and_has_no_columns()
    {
        var catalog = MigrationSamples.Catalog(("public", "orders", ["order_id"]));

        Assert.False(catalog.Knows("customers"));
        Assert.Empty(catalog.Columns("customers"));
        Assert.False(catalog.HasColumn("customers", "id"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Bare_name_declared_under_two_schemas_is_ambiguous_until_qualified()
    {
        var catalog = MigrationSamples.Catalog(
            ("dbo", "orders", ["order_id"]),
            ("sales", "orders", ["sales_order_id"]));

        Assert.True(catalog.IsAmbiguous("orders"));
        Assert.False(catalog.Knows("orders"));
        Assert.Equal(["dbo.orders", "sales.orders"], catalog.Candidates("orders"));

        // Qualifying resolves it.
        Assert.False(catalog.IsAmbiguous("dbo.orders"));
        Assert.True(catalog.Knows("dbo.orders"));
        Assert.True(catalog.HasColumn("sales.orders", "sales_order_id"));
        Assert.False(catalog.HasColumn("dbo.orders", "sales_order_id"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Rendered_ddl_round_trips_through_the_extractor()
    {
        var table = new CdcSinkSourceTable
        {
            SourceTableSchema = "public",
            SourceTableName = "orders",
            PrimaryKeyColumns = ["order_id"],
            Columns =
            [
                new CdcSinkSourceColumn { Name = "order_id", NativeType = "integer" },
                new CdcSinkSourceColumn { Name = "ordered_at", NativeType = "timestamp" },
                new CdcSinkSourceColumn { Name = "metadata", NativeType = "jsonb", SuggestedType = CdcColumnType.Json }
            ],
            ForeignKeys =
            [
                new CdcSinkSourceForeignKey
                {
                    Columns = ["customer_id"],
                    ReferencedSchema = "public",
                    ReferencedTable = "customers",
                    ReferencedColumns = ["customer_id"]
                }
            ]
        };

        var declared = Assert.Single(DdlColumnExtractor.Extract(DdlRenderer.Render(table)));

        Assert.Equal("public", declared.Schema);
        Assert.Equal("orders", declared.Name);
        Assert.Equal(["order_id", "ordered_at", "metadata"], declared.Columns);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("CREATE TABLE orders (order_id int, note varchar(50));", "", "orders", "order_id,note")]
    [InlineData("CREATE TABLE dbo.[orders] ([order id] int, [note] nvarchar(max));", "dbo", "orders", "order id,note")]
    [InlineData("create table if not exists \"public\".\"orders\" (\"order_id\" int);", "public", "orders", "order_id")]
    [InlineData("CREATE TABLE `shop`.`orders` (`order_id` INT, PRIMARY KEY (`order_id`));", "shop", "orders", "order_id")]
    public void Extractor_reads_table_and_columns(string ddl, string expectedSchema, string expectedTable, string expectedColumns)
    {
        var declared = Assert.Single(DdlColumnExtractor.Extract(ddl));

        Assert.Equal(expectedSchema, declared.Schema);
        Assert.Equal(expectedTable, declared.Name);
        Assert.Equal(expectedColumns.Split(','), declared.Columns);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Extractor_skips_table_level_constraints()
    {
        const string ddl = """
            CREATE TABLE public.order_lines (
                order_line_id integer NOT NULL,
                order_id integer NOT NULL,
                quantity numeric(10, 2) DEFAULT 0,
                CONSTRAINT pk_order_lines PRIMARY KEY (order_line_id),
                FOREIGN KEY (order_id) REFERENCES public.orders (order_id),
                UNIQUE (order_id, order_line_id),
                CHECK (quantity > 0)
            );
            """;

        var declared = Assert.Single(DdlColumnExtractor.Extract(ddl));

        Assert.Equal(["order_line_id", "order_id", "quantity"], declared.Columns);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Unparsable_ddl_leaves_the_table_unknown_rather_than_half_known()
    {
        var path = Path.Combine(Path.GetTempPath(), $"rvn-migration-test-{Guid.NewGuid():N}.sql");
        File.WriteAllText(path, "-- a comment and nothing this extractor understands\nSELECT 1;");

        try
        {
            var catalog = SchemaCatalog.FromFiles([path]);

            Assert.False(catalog.Knows("orders"));
            Assert.Single(catalog.Files);
            Assert.NotEmpty(catalog.Files[0].Digest);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Discovered_files_carry_content_and_open_without_touching_disk()
    {
        var catalog = MigrationSamples.Catalog(("public", "orders", ["order_id"]));
        var file = Assert.Single(catalog.Files);

        Assert.Equal("public.orders.sql", file.Name);
        Assert.Null(file.Path);

        using var reader = new StreamReader(file.OpenRead());
        Assert.Contains("CREATE TABLE public.orders", reader.ReadToEnd());
    }
}

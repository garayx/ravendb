using FastTests;
using Raven.Quill.AiHelper.Migration.Planning;
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
    public void Discovered_files_carry_their_rendered_ddl()
    {
        var catalog = MigrationSamples.Catalog(("public", "orders", ["order_id"]));
        var file = Assert.Single(catalog.Files);

        Assert.Equal("public.orders.sql", file.Name);

        using var reader = new StreamReader(file.OpenRead());
        Assert.Contains("CREATE TABLE public.orders", reader.ReadToEnd());
    }
}

using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Planning;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationPlanValidatorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static ValidationResult Validate(CdcSinkTableConfig? config, MigrationPlan? plan = null, SchemaCatalog? catalog = null) =>
        PlanValidator.Validate("Orders", config, plan ?? new MigrationPlan(), catalog ?? MigrationSamples.OrdersCatalog());

    [RavenFact(RavenTestCategory.Quill)]
    public void A_config_grounded_in_the_schema_is_accepted()
    {
        var config = MigrationSamples.ValidOrders();
        config.EmbeddedTables = [MigrationSamples.ValidLines()];

        var result = Validate(config);

        Assert.True(result.Ok);
        Assert.Empty(result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_broken_config_is_rejected_with_the_exact_error_list()
    {
        var config = MigrationSamples.ValidOrders();
        config.PrimaryKeyColumns = ["order_id"];
        config.Columns =
        [
            new CdcColumnMapping { Column = "ordered_at", Name = "OrderedAt" },
            new CdcColumnMapping { Column = "region", Name = "Region" }
        ];

        var result = Validate(config);

        Assert.Equal(
        [
            "root 'public.orders': primary key column 'order_id' is not present in Columns.",
            "root 'public.orders': column 'region' does not exist in the source schema. " +
            "Known columns: order_id, ordered_at, customer_id."
        ], result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_schema_written_into_an_embedded_table_name_is_rejected()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = null;
        lines.SourceTableName = "public.order_lines";
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.Contains(result.Errors, e =>
            e.Contains("'public.order_lines' includes a schema") &&
            e.Contains("SourceTableSchema to 'public' and SourceTableName to 'order_lines'"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_column_mapped_twice_is_rejected_at_registration_not_at_apply()
    {
        var config = MigrationSamples.ValidOrders();
        config.Columns.Add(new CdcColumnMapping { Column = "order_id", Name = "OrderIdAgain" });

        var result = Validate(config);

        Assert.Contains(result.Errors, e => e.Contains("duplicate column 'order_id'"));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("snake_case", PropertyCase.SnakeCase)]
    [InlineData("SnakeCase", PropertyCase.SnakeCase)]
    [InlineData("snake-case", PropertyCase.SnakeCase)]
    [InlineData("camel case", PropertyCase.CamelCase)]
    [InlineData("camelCase", PropertyCase.CamelCase)]
    [InlineData("Pascal", PropertyCase.PascalCase)]
    [InlineData("PASCAL_CASE", PropertyCase.PascalCase)]
    public void A_property_case_is_read_however_it_is_spelled(string value, PropertyCase expected)
    {
        Assert.True(PropertyCases.TryParse(value, out var parsed));
        Assert.Equal(expected, parsed);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("kebab-case")]
    [InlineData("Unspecified")]
    public void An_unknown_property_case_is_refused(string? value)
    {
        Assert.False(PropertyCases.TryParse(value, out _));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Missing_config_is_reported_rather_than_throwing()
    {
        var result = Validate(config: null);

        Assert.Equal(["Config is missing."], result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Collection_name_must_match_the_registered_name()
    {
        var config = MigrationSamples.ValidOrders();
        config.CollectionName = "Order";

        var result = Validate(config);

        Assert.Contains("Collection 'Orders' does not match Config.CollectionName 'Order'.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Empty_columns_and_missing_primary_key_are_both_errors()
    {
        var config = MigrationSamples.ValidOrders();
        config.Columns = [];
        config.PrimaryKeyColumns = [];

        var result = Validate(config);

        Assert.Contains("root 'public.orders': Columns is empty - a mapping with no columns produces empty documents.", result.Errors);
        Assert.Contains("root 'public.orders': PrimaryKeyColumns is required - document IDs are derived from it.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_primary_key_mapped_as_an_attachment_is_an_error()
    {
        var config = MigrationSamples.ValidOrders();
        config.Columns[0].Type = CdcColumnType.Attachment;

        var result = Validate(config);

        Assert.Contains("root 'public.orders': primary key column 'order_id' is mapped as an Attachment.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Two_columns_mapping_to_one_property_is_an_error()
    {
        var config = MigrationSamples.ValidOrders();
        config.Columns[1].Name = "OrderId";

        var result = Validate(config);

        Assert.Contains("root 'public.orders': property name 'OrderId' is mapped 2 times (from order_id, ordered_at).", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Property_names_must_follow_the_agreed_convention()
    {
        var plan = new MigrationPlan();
        plan.SetConventions(new NamingConventions(PropertyCase.SnakeCase));

        var result = Validate(MigrationSamples.ValidOrders(), plan);

        Assert.Contains("root 'public.orders': property 'OrderId' does not follow the agreed SnakeCase convention.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_unspecified_convention_accepts_any_casing()
    {
        var result = Validate(MigrationSamples.ValidOrders());

        Assert.DoesNotContain(result.Errors, e => e.Contains("convention"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_ambiguous_bare_table_name_names_both_candidates()
    {
        var catalog = MigrationSamples.Catalog(
            ("dbo", "orders", ["order_id"]),
            ("sales", "orders", ["order_id"]));

        var config = MigrationSamples.ValidOrders();
        config.SourceTableSchema = null;
        config.Columns = [new CdcColumnMapping { Column = "order_id", Name = "OrderId" }];

        var result = Validate(config, plan: null, catalog);

        Assert.Contains(
            "root 'orders': the source schema declares more than one table with this name " +
            "(dbo.orders, sales.orders). Set SourceTableSchema to say which one.",
            result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_without_join_columns_is_rejected()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.JoinColumns = [];
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.Contains(
            "embedded 'public.order_lines' in Orders: JoinColumns is required - without it CDC Sink " +
            "cannot tell which document a changed row belongs to.",
            result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_join_column_must_exist_on_the_child_table()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.JoinColumns = ["bogus"];
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.Contains("embedded 'public.order_lines' in Orders: join column 'bogus' does not exist on 'public.order_lines'.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_inherits_its_root_schema_when_it_names_none()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = null;
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.True(result.Ok, string.Join(" | ", result.Errors));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_linked_join_column_must_exist_on_the_parent_table()
    {
        var config = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["bogus"];
        config.LinkedTables = [customer];

        var result = Validate(config);

        Assert.Contains("linked 'customers' in Orders: join column 'bogus' does not exist on the parent table 'public.orders'.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_link_needs_a_property_name_a_target_collection_and_join_columns()
    {
        var config = MigrationSamples.ValidOrders();
        config.LinkedTables =
        [
            new CdcSinkLinkedTableConfig { SourceTableSchema = "public", SourceTableName = "customers" }
        ];

        var result = Validate(config);

        Assert.Contains("linked 'customers' in Orders: PropertyName is required.", result.Errors);
        Assert.Contains("linked 'customers' in Orders: LinkedCollectionName is required - it forms the referenced document ID.", result.Errors);
        Assert.Contains("linked 'customers' in Orders: JoinColumns is required.", result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_root_patch_reading_an_embedded_property_is_rejected_as_stale()
    {
        var config = MigrationSamples.ValidOrders();
        config.EmbeddedTables = [MigrationSamples.ValidLines()];
        config.Patch = "this.Total = (this.Lines || []).reduce((s, l) => s + l.Quantity, 0);";

        var result = Validate(config);

        Assert.Contains(
            "Orders: the root Patch reads this.Lines, which is maintained by the embedded 'order_lines' " +
            "mapping. A root patch only runs when the root row changes, so this value goes stale. " +
            "Move it to the 'order_lines' mapping's Patch.",
            result.Errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_same_patch_on_the_child_mapping_is_accepted()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.Patch = "this.Total = (this.Lines || []).reduce((s, l) => s + l.Quantity, 0);";
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.True(result.Ok, string.Join(" | ", result.Errors));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_link_to_a_collection_nothing_produces_warns_without_blocking()
    {
        var config = MigrationSamples.ValidOrders();
        config.LinkedTables = [MigrationSamples.ValidCustomer()];

        var result = Validate(config);

        Assert.True(result.Ok);
        Assert.Contains(
            "linked 'customers' in Orders: nothing in the plan produces the 'Customers' collection yet, " +
            "so these references will dangle until it is registered.",
            result.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_join_column_count_that_does_not_match_the_parent_key_warns()
    {
        var config = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.JoinColumns = ["order_id", "order_line_id"];
        config.EmbeddedTables = [lines];

        var result = Validate(config);

        Assert.True(result.Ok);
        Assert.Contains("embedded 'public.order_lines' in Orders: 2 join column(s) against a parent key of 1 column(s) on 'public.orders'.", result.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Embedding_the_same_table_in_a_second_collection_warns()
    {
        var plan = new MigrationPlan();
        var invoices = MigrationSamples.ValidOrders();
        invoices.CollectionName = "Invoices";
        invoices.EmbeddedTables = [MigrationSamples.ValidLines()];
        plan.Upsert("Invoices", rationale: null, invoices);

        var config = MigrationSamples.ValidOrders();
        config.EmbeddedTables = [MigrationSamples.ValidLines()];

        var result = Validate(config, plan);

        Assert.True(result.Ok);
        Assert.Contains(
            "table 'public.order_lines' is also used as: embedded in Invoices. Embedding the same rows in " +
            "more than one document means more than one copy to keep consistent - intended?",
            result.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_table_that_is_a_root_elsewhere_and_only_linked_here_does_not_warn()
    {
        var plan = new MigrationPlan();
        var customers = new CdcSinkTableConfig
        {
            CollectionName = "Customers",
            SourceTableSchema = "public",
            SourceTableName = "customers",
            PrimaryKeyColumns = ["customer_id"],
            Columns = [new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" }]
        };
        plan.Upsert("Customers", rationale: null, customers);

        var config = MigrationSamples.ValidOrders();
        config.LinkedTables = [MigrationSamples.ValidCustomer()];

        var result = Validate(config, plan);

        Assert.True(result.Ok);
        Assert.DoesNotContain(result.Warnings, w => w.Contains("more than one copy"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Columns_absent_from_an_unparsed_schema_are_not_rejected()
    {
        var config = MigrationSamples.ValidOrders();
        config.Columns.Add(new CdcColumnMapping { Column = "anything", Name = "Anything" });

        var result = Validate(config, plan: null, MigrationSamples.Catalog());

        Assert.True(result.Ok, string.Join(" | ", result.Errors));
    }
}

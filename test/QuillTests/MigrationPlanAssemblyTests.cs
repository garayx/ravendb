using System.Threading.Channels;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Agent;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationPlanAssemblyTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static MigrationPlanSnapshot Snapshot(params CdcSinkTableConfig[] configs) =>
        new("MigrationChats/abc", "shop", "key", PropertyCase.Unspecified, null,
            configs.Select((c, i) => new MigrationPlanCollection(c.CollectionName, i + 1, null, c)).ToArray(),
            []);

    private static CdcSinkSourceSchema Discovered(params string[] tables) => new()
    {
        CatalogName = "shop",
        Tables = tables.Select(t => new CdcSinkSourceTable
        {
            SourceTableSchema = "public",
            SourceTableName = t
        }).ToList()
    };

    [RavenFact(RavenTestCategory.Quill)]
    public void Build_carries_every_registered_configuration()
    {
        var config = PlanToCdcConfiguration.Build(
            Snapshot(MigrationSamples.ValidOrders()), "shop-cdc", "quill-cdc-connection");

        Assert.Equal("shop-cdc", config.Name);
        Assert.Equal("quill-cdc-connection", config.ConnectionStringName);
        Assert.Equal("Orders", Assert.Single(config.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_counts_as_covered_but_a_linked_one_does_not()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.EmbeddedTables = [MigrationSamples.ValidLines()];
        orders.LinkedTables = [MigrationSamples.ValidCustomer()];

        var config = PlanToCdcConfiguration.Build(Snapshot(orders), "shop-cdc", "cs");
        var unmapped = PlanToCdcConfiguration.UnmappedTables(
            config, Discovered("orders", "order_lines", "customers"));

        // orders is a root and order_lines is embedded, so both are produced; customers is only
        // referenced, so nothing in this plan writes it.
        Assert.Equal(["public.customers"], unmapped);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_disabled_root_is_not_covered()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.Disabled = true;

        var config = PlanToCdcConfiguration.Build(Snapshot(orders), "shop-cdc", "cs");

        Assert.Equal(["public.orders"], PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders")));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_without_its_own_schema_is_read_against_the_discovered_one()
    {
        var orders = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = null;
        orders.EmbeddedTables = [lines];

        var config = PlanToCdcConfiguration.Build(Snapshot(orders), "shop-cdc", "cs");

        Assert.Empty(PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders", "order_lines")));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Full_coverage_reports_nothing()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.EmbeddedTables = [MigrationSamples.ValidLines()];

        var config = PlanToCdcConfiguration.Build(Snapshot(orders), "shop-cdc", "cs");

        Assert.Empty(PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders", "order_lines")));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_channel_turns_plan_changes_into_frames()
    {
        var queue = Channel.CreateUnbounded<MigrationFrame>();
        var channel = new QueuedPlanChannel(queue.Writer);

        var plan = new MigrationPlan();
        var entry = plan.Upsert("Orders", "because", MigrationSamples.ValidOrders());

        channel.ProposalRegistered(new ProposePlanArgs
        {
            Collections = [new ProposedCollection { Collection = "Orders", RootTable = "orders" }],
            Dropped = [new DroppedTable { Table = "order_tags", Why = "pure join table" }]
        });
        channel.CollectionRegistered(entry, ["a warning"]);
        channel.CollectionRejected("Products", ["an error"]);
        channel.CollectionRemoved("Products", "dropped");
        channel.ConventionsChanged(new NamingConventions(PropertyCase.SnakeCase, "Spanish"), ["Orders"]);
        channel.Note("something happened");
        queue.Writer.Complete();

        var frames = queue.Reader.ReadAllAsync().ToBlockingEnumerable().ToArray();

        var proposal = Assert.IsType<ProposalFrame>(frames[0]);
        Assert.Equal("Orders", Assert.Single(proposal.Collections).Collection);
        Assert.Equal("order_tags", Assert.Single(proposal.Dropped).Table);

        var collection = Assert.IsType<CollectionFrame>(frames[1]);
        Assert.Equal("registered", collection.Status);
        Assert.Equal("Orders", collection.Collection);
        Assert.Equal(1, collection.Version);
        Assert.Equal("because", collection.Rationale);
        Assert.Equal(["a warning"], collection.Warnings);

        var rejected = Assert.IsType<RejectedFrame>(frames[2]);
        Assert.Equal("Products", rejected.Collection);
        Assert.Equal(["an error"], rejected.Errors);

        Assert.Equal("Products", Assert.IsType<RemovedFrame>(frames[3]).Collection);

        var conventions = Assert.IsType<ConventionsFrame>(frames[4]);
        Assert.Equal(PropertyCase.SnakeCase, conventions.PropertyCase);
        Assert.Equal("Spanish", conventions.PropertyLanguage);
        Assert.Equal(["Orders"], conventions.MustReEmit);

        Assert.Equal("something happened", Assert.IsType<NoteFrame>(frames[5]).Text);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Re_registering_a_collection_reports_it_as_replaced()
    {
        var queue = Channel.CreateUnbounded<MigrationFrame>();
        var channel = new QueuedPlanChannel(queue.Writer);

        var plan = new MigrationPlan();
        plan.Upsert("Orders", null, MigrationSamples.ValidOrders());
        var second = plan.Upsert("Orders", null, MigrationSamples.ValidOrders());

        channel.CollectionRegistered(second, []);
        queue.Writer.Complete();

        var frame = Assert.IsType<CollectionFrame>(queue.Reader.ReadAllAsync().ToBlockingEnumerable().Single());
        Assert.Equal("replaced", frame.Status);
        Assert.Equal(2, frame.Version);
        Assert.Single(plan.CollectionNames());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_iteration_budget_scales_with_the_schema_and_is_capped()
    {
        Assert.Equal(36, MigrationSession.IterationBudgetFor(10));
        Assert.Equal(256, MigrationSession.IterationBudgetFor(300));
    }
}

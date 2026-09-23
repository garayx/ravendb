using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_streams_the_frames_the_planner_produced()
    {
        var client = new FakeMigrationClient();
        client.OnStart = frames =>
        {
            frames.Add(new ProposalFrame
            {
                Collections = [new() { Collection = "Orders", RootTable = "orders" }]
            });
            frames.Add(new CollectionFrame
            {
                Status = "registered",
                Collection = "Orders",
                Version = 1,
                Config = MigrationSamples.ValidOrders()
            });
            frames.Add(new DoneFrame { ConversationId = "MigrationChats/abc", InputKey = "key" });
        };

        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("application/x-ndjson", resp.Content.Headers.ContentType?.MediaType);

        var frames = await ReadFramesAsync(resp);

        Assert.Equal(["proposal", "collection", "done"], frames.Select(f => (string?)f["type"]));
        Assert.Equal("Orders", (string?)frames[1]["collection"]);
        Assert.Equal("MigrationChats/abc", (string?)frames[2]["conversationId"]);

        // the schema the planner was handed is the one discovery stored
        Assert.Equal(3, client.LastStart!.Schema.Tables.Count);
        Assert.Equal(MigrationService.DefaultStartPrompt, client.LastStart.Prompt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_narrows_the_schema_to_the_selected_tables()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStart, new
        {
            slug = QuillHost.DefaultWizardSlug,
            selectedTables = new[] { new { sourceTableName = "orders" } },
            prompt = "just orders please"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("orders", Assert.Single(client.LastStart!.Schema.Tables).SourceTableName);
        Assert.Equal("just orders please", client.LastStart.Prompt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_without_a_discovered_schema_is_refused_before_the_planner_is_called()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("discover", error!.Error);
        Assert.Null(client.LastStart);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_without_consent_is_401_and_the_planner_is_not_called()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client, AiHelperStatus.ConsentRequired);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Null(client.LastStart);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unreachable_ai_service_is_502_not_a_consent_problem()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client, AiHelperStatus.InternalError);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        // The operator cannot fix this by giving consent, so it must not be reported as consent.
        Assert.Equal(HttpStatusCode.BadGateway, resp.StatusCode);
        Assert.Null(client.LastStart);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_refuses_a_conversation_that_belongs_to_another_app()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor("someone-else") };
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc",
            prompt = "go ahead with orders"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("no planning session", error!.Error);
        Assert.Null(client.LastAsk);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_refuses_a_conversation_that_has_no_planning_session()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/never-started",
            prompt = "go ahead with orders"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Null(client.LastAsk);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Fork_requires_an_input_key()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationFork, new
        {
            slug = QuillHost.DefaultWizardSlug,
            inputKey = "",
            branch = "b"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Fork_refuses_an_analysis_that_belongs_to_another_app()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);
        await SeedCheckpointAsync(host, "someone-elses-key", slug: "someone-else");

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationFork, new
        {
            slug = QuillHost.DefaultWizardSlug,
            inputKey = "someone-elses-key",
            branch = "b"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("different app", error!.Error);

        // The refusal has to land before the client copies the checkpoint's DDL and proposal.
        Assert.Null(client.LastFork);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Fork_branches_an_analysis_of_its_own_app()
    {
        var client = new FakeMigrationClient();
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);
        await SeedCheckpointAsync(host, "my-key", QuillHost.DefaultWizardSlug);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationFork, new
        {
            slug = QuillHost.DefaultWizardSlug,
            inputKey = "my-key",
            branch = "b"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("my-key", client.LastFork!.InputKey);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_plan_snapshot_is_readable_by_its_owning_app()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor(QuillHost.DefaultWizardSlug) };
        await using var host = await NewMigrationHostAsync(client);

        var snapshot = await host.Client.GetFromJsonAsync<MigrationPlanSnapshot>(
            $"{QuillRoutes.MigrationPlan("MigrationChats/abc")}?slug={QuillHost.DefaultWizardSlug}");

        Assert.NotNull(snapshot);
        Assert.Equal("Orders", Assert.Single(snapshot.Collections).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_plan_snapshot_is_not_readable_by_another_app()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor("someone-else") };
        await using var host = await NewMigrationHostAsync(client);

        var resp = await host.Client.GetAsync(
            $"{QuillRoutes.MigrationPlan("MigrationChats/abc")}?slug={QuillHost.DefaultWizardSlug}");

        // A mismatch reads as "no such plan" rather than admitting the plan exists.
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_plan_snapshot_requires_a_slug()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor(QuillHost.DefaultWizardSlug) };
        await using var host = await NewMigrationHostAsync(client);

        var resp = await host.Client.GetAsync(QuillRoutes.MigrationPlan("MigrationChats/abc"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_narrows_the_configuration_to_the_selected_collections()
    {
        var customers = MigrationSamples.ValidOrders();
        customers.CollectionName = "Customers";
        customers.SourceTableName = "customers";
        customers.PrimaryKeyColumns = ["order_id"];

        var client = new FakeMigrationClient
        {
            Snapshot = new MigrationPlanSnapshot("MigrationChats/abc", QuillHost.DefaultWizardSlug, "key",
                PropertyCase.Unspecified, null,
                [
                    new MigrationPlanCollection("Orders", 1, null, MigrationSamples.ValidOrders()),
                    new MigrationPlanCollection("Customers", 1, null, customers)
                ],
                [])
        };

        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc",
            collections = new[] { "Orders" }
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>();
        Assert.Equal("Orders", Assert.Single(applied!.Configuration!.Tables).CollectionName);

        // A collection left behind is reported rather than silently dropped.
        Assert.Contains("public.customers", applied.UnmappedTables);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_refuses_a_collection_the_plan_does_not_hold()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor(QuillHost.DefaultWizardSlug) };
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc",
            collections = new[] { "Orders", "Invented" }
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("Invented", error!.Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_assembles_the_plan_persists_it_and_reports_uncovered_tables()
    {
        var client = new FakeMigrationClient { Snapshot = SnapshotFor(QuillHost.DefaultWizardSlug) };
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>();
        Assert.NotNull(applied);
        Assert.Equal("Orders", Assert.Single(applied.Configuration!.Tables).CollectionName);

        // only orders is mapped; the other two discovered tables are reported rather than dropped silently
        Assert.Equal(["public.customers", "public.audit_log"], applied.UnmappedTables);

        using var session = host.Config.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
        Assert.NotNull(state!.LastMapConfiguration);
        Assert.Equal("Orders", Assert.Single(state.LastMapConfiguration.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_rejects_a_plan_that_does_not_assemble_into_a_valid_configuration()
    {
        var broken = MigrationSamples.ValidOrders();
        broken.PrimaryKeyColumns = [];

        var client = new FakeMigrationClient { Snapshot = SnapshotFor(QuillHost.DefaultWizardSlug, broken) };
        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc"
        });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_refuses_an_empty_plan()
    {
        var client = new FakeMigrationClient
        {
            Snapshot = new MigrationPlanSnapshot("MigrationChats/abc", QuillHost.DefaultWizardSlug, null,
                PropertyCase.Unspecified, null, [], [])
        };

        await using var host = await NewMigrationHostAsync(client);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/abc"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // -------------------------------------------------------------------

    private Task<QuillHost> NewMigrationHostAsync(
        FakeMigrationClient client,
        AiHelperStatus consent = AiHelperStatus.Success) =>
        NewHostAsync(configureServices: services =>
        {
            services.RemoveAll<IMigrationClient>();
            services.AddSingleton<IMigrationClient>(client);
            services.RemoveAll<IAiHelperClient>();
            services.AddSingleton<IAiHelperClient>(new FakeConsentClient(consent));
        });

    private static async Task<List<JsonNode>> ReadFramesAsync(HttpResponseMessage resp)
    {
        var body = await resp.Content.ReadAsStringAsync();

        return body
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonNode.Parse(line)!)
            .ToList();
    }

    private static MigrationPlanSnapshot SnapshotFor(string slug, CdcSinkTableConfig? config = null) =>
        new("MigrationChats/abc", slug, "key", PropertyCase.Unspecified, null,
            [new MigrationPlanCollection("Orders", 1, "because", config ?? MigrationSamples.ValidOrders())],
            []);

    private static async Task SeedCheckpointAsync(QuillHost host, string inputKey, string slug)
    {
        using var session = host.Config.OpenAsyncSession();
        await session.StoreAsync(
            new PlanCheckpoint
            {
                Id = PlanCheckpoint.DocumentId(inputKey),
                InputKey = inputKey,
                Slug = slug,
                AgentIdentifier = "test-agent",
                CreatedAt = DateTime.UtcNow
            },
            PlanCheckpoint.DocumentId(inputKey));

        await session.SaveChangesAsync();
    }

    private static async Task SeedDiscoveredSchemaAsync(QuillHost host)
    {
        using var session = host.Config.OpenAsyncSession();
        await session.StoreAsync(new WizardState
        {
            Provider = "SqlClient",
            LastDiscoveredSchema = new CdcSinkSourceSchema
            {
                CatalogName = "shop",
                HasPermissionToSetup = true,
                Tables = [SourceTable("orders"), SourceTable("customers"), SourceTable("audit_log")]
            },
            LastDiscoverAt = DateTime.UtcNow
        }, WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));

        await session.SaveChangesAsync();
    }

    private static CdcSinkSourceTable SourceTable(string name) => new()
    {
        SourceTableSchema = "public",
        SourceTableName = name,
        IsCdcEnabled = true,
        PrimaryKeyColumns = ["order_id"],
        Columns =
        [
            new CdcSinkSourceColumn { Name = "order_id", NativeType = "int", IsPrimaryKey = true, IsCdcCapturable = true },
            new CdcSinkSourceColumn { Name = "ordered_at", NativeType = "timestamp", IsCdcCapturable = true }
        ]
    };

    private sealed class FakeMigrationClient : IMigrationClient
    {
        public Action<List<MigrationFrame>>? OnStart { get; set; }
        public MigrationStartCommand? LastStart { get; private set; }
        public MigrationAskCommand? LastAsk { get; private set; }
        public MigrationForkCommand? LastFork { get; private set; }
        public MigrationPlanSnapshot? Snapshot { get; set; }

        public async Task StartAsync(MigrationStartCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token)
        {
            LastStart = command;
            await EmitAsync(onFrame);
        }

        public async Task AskAsync(MigrationAskCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token)
        {
            LastAsk = command;
            await EmitAsync(onFrame);
        }

        public async Task ForkAsync(MigrationForkCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token)
        {
            LastFork = command;
            await EmitAsync(onFrame);
        }

        public Task<MigrationPlanSnapshot?> GetAsync(string slug, string conversationId, CancellationToken token) =>
            Task.FromResult(
                Snapshot is not null && string.Equals(Snapshot.Slug, slug, StringComparison.OrdinalIgnoreCase)
                    ? Snapshot
                    : null);

        private async Task EmitAsync(Func<MigrationFrame, Task> onFrame)
        {
            var frames = new List<MigrationFrame>();
            OnStart?.Invoke(frames);

            if (frames.Count == 0)
                frames.Add(new DoneFrame { ConversationId = "MigrationChats/abc" });

            foreach (var frame in frames)
                await onFrame(frame);
        }
    }

    private sealed class FakeConsentClient(AiHelperStatus consent) : IAiHelperClient
    {
        public Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct) => Task.FromResult(consent);

        public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) => Task.FromResult(AiHelperStatus.Success);

        public Task<SuggestCdcInternalResult> SuggestCdcAsync(object? schema, object? samples, string prompt, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
            CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class =>
            Task.FromResult(JsonSerializer.Deserialize<T>(json)!);
    }
}

using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Agent;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints;
using Raven.Quill.Wizard;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// The Quill half of a planning session: which app it belongs to, which schema it reads, whether
/// the operator has consented, and what to do with the plan once they are happy with it. The
/// conversation itself belongs to <see cref="IMigrationClient"/>.
/// </summary>
public sealed class MigrationService(
    IDocumentStore store,
    IMigrationClient client,
    IAiHelperClient aiClient)
{
    /// <summary>
    /// The opening message from the design: the agent groups the tables and explains itself, and
    /// registers nothing until the operator has chosen.
    /// </summary>
    public const string DefaultStartPrompt =
        "Look at the set of tables, propose key areas to migrate, the specific collections to create " +
        "(and the tables they include). Explain briefly the reasoning and what sort of agents and " +
        "behaviours this allows.";

    /// <summary>
    /// Why a request was turned away. <see cref="Status"/> is set only when the AI service is the
    /// reason, so the endpoint can tell "you have not consented" apart from "the service is down".
    /// </summary>
    public sealed record Refusal(string Message, AiHelperStatus? Status = null);

    public async Task<Refusal?> StartAsync(
        MigrationStartRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
        var (schema, refusal) = await ResolveSchemaAsync(request.Slug, request.SelectedTables, token);
        if (refusal is not null)
            return refusal;

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        var prompt = string.IsNullOrWhiteSpace(request.Prompt) ? DefaultStartPrompt : request.Prompt!;

        await client.StartAsync(new MigrationStartCommand(request.Slug, schema!, prompt), onFrame, token);

        return null;
    }

    public async Task<Refusal?> AskAsync(
        MigrationAskRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return new Refusal("conversationId is required");
        if (string.IsNullOrWhiteSpace(request.Prompt))
            return new Refusal("prompt is required");

        var (schema, refusal) = await ResolveSchemaAsync(request.Slug, selected: null, token);
        if (refusal is not null)
            return refusal;

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        // Every session persists its plan before this can be called - start does it at the end of
        // the opening turn, fork does it as it branches - so nothing found here means the
        // conversation is not this app's to continue, whether or not it exists at all.
        if (await client.GetAsync(request.Slug, request.ConversationId, token) is null)
            return new Refusal("no planning session found for that conversation");

        await client.AskAsync(
            new MigrationAskCommand(request.Slug, request.ConversationId, schema!, request.Prompt), onFrame, token);

        return null;
    }

    public async Task<Refusal?> ForkAsync(
        MigrationForkRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.InputKey))
            return new Refusal("inputKey is required");

        // The branch validates against the discovered schema, same as the session it came from,
        // rather than against DDL read back off the checkpoint.
        var (schema, refusal) = await ResolveSchemaAsync(request.Slug, selected: null, token);
        if (refusal is not null)
            return refusal;

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        // Checked here rather than in the client, because forking copies the checkpoint's DDL and
        // its proposal into the new conversation - by the time the client has it, another app's
        // analysis has already been handed over.
        var checkpoint = await MigrationSession.FindCheckpointAsync(store, request.InputKey, token);

        if (checkpoint is null)
            return new Refusal($"no stored analysis found for input key '{request.InputKey}'");

        // Checkpoints written before checkpoints had an owner are treated as unowned rather than
        // stranded.
        if (string.IsNullOrEmpty(checkpoint.Slug) == false &&
            string.Equals(checkpoint.Slug, request.Slug, StringComparison.OrdinalIgnoreCase) == false)
        {
            return new Refusal("that analysis belongs to a different app");
        }

        var branch = string.IsNullOrWhiteSpace(request.Branch) ? "branch" : request.Branch;

        await client.ForkAsync(
            new MigrationForkCommand(request.Slug, schema!, request.InputKey, branch), onFrame, token);

        return null;
    }

    public Task<MigrationPlanSnapshot?> GetAsync(string slug, string conversationId, CancellationToken token) =>
        client.GetAsync(slug, conversationId, token);

    /// <summary>
    /// Turns the registered plan into the configuration the wizard carries on with. The per-call
    /// validation the model saw is not enough on its own: only the assembled configuration can be
    /// checked for names colliding across a table's own mappings, and for join columns that name a
    /// mapped property instead of a source column.
    /// </summary>
    public async Task<(MigrationApplyResponse? Response, Refusal? Refusal)> ApplyAsync(
        MigrationApplyRequest request,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return (null, new Refusal("conversationId is required"));

        var snapshot = await client.GetAsync(request.Slug, request.ConversationId, token);
        if (snapshot is null)
            return (null, new Refusal("no plan found for that conversation"));

        if (snapshot.Collections.Length == 0)
            return (null, new Refusal("the plan has no collections yet"));

        var (selected, unknown) = SelectCollections(snapshot, request.Collections);

        if (unknown.Length > 0)
            return (null, new Refusal($"the plan has no collection named {string.Join(", ", unknown)}"));

        if (selected.Length == 0)
            return (null, new Refusal("no collections were selected"));

        snapshot = snapshot with { Collections = selected };

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);

        if (state?.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        var configuration = PlanToCdcConfiguration.Build(
            snapshot,
            state.LastMapConfiguration?.Name ?? string.Empty,
            state.LastMapConfiguration?.ConnectionStringName ?? string.Empty);

        if (configuration.Validate(out var errors, validateName: false, validateConnection: false) == false)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        WizardEndpoints.ValidateJoinColumnsAgainstSchema(configuration, state.LastDiscoveredSchema, errors);
        if (errors.Count > 0)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        var unmapped = PlanToCdcConfiguration.UnmappedTables(configuration, state.LastDiscoveredSchema);

        state.LastMapConfiguration = configuration;
        state.LastMapAt = DateTime.UtcNow;
        await session.SaveChangesAsync(token);

        return (new MigrationApplyResponse(configuration, unmapped, []), null);
    }

    /// <summary>
    /// Narrows the plan to the collections the operator kept. Naming one the plan does not hold is
    /// a mistake worth reporting rather than quietly ignoring - it usually means the caller is
    /// working from a stale view of the plan.
    /// </summary>
    private static (MigrationPlanCollection[] Selected, string[] Unknown) SelectCollections(
        MigrationPlanSnapshot snapshot,
        string[]? requested)
    {
        if (requested is not { Length: > 0 })
            return (snapshot.Collections, []);

        var wanted = requested.ToHashSet(StringComparer.OrdinalIgnoreCase);

        var unknown = wanted
            .Where(name => snapshot.Collections.Any(c => string.Equals(c.Collection, name, StringComparison.OrdinalIgnoreCase)) == false)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        return (snapshot.Collections.Where(c => wanted.Contains(c.Collection)).ToArray(), unknown);
    }

    private async Task<(CdcSinkSourceSchema? Schema, Refusal? Refusal)> ResolveSchemaAsync(
        string slug,
        SelectedSourceTable[]? selected,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(slug))
            return (null, new Refusal("slug is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), token);

        if (state?.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        if (selected is not { Length: > 0 })
            return (state.LastDiscoveredSchema, null);

        var narrowed = WizardEndpoints.SelectTables(state.LastDiscoveredSchema, selected);

        return narrowed.Tables.Count == 0
            ? (null, new Refusal("none of the selected tables are part of the discovered schema"))
            : (narrowed, null);
    }

    /// <summary>
    /// The same gate the one-shot path runs. It stays on the local path too: leaving it to the
    /// remote implementation would quietly drop it for as long as the agent runs in-process.
    /// </summary>
    private async Task<Refusal?> RequireConsentAsync(CancellationToken token)
    {
        var status = await aiClient.CheckConsentAsync(token);

        if (status == AiHelperStatus.Success)
            return null;

        return status.ServiceAnswered()
            ? new Refusal("consent to the RavenDB AI service is required", status)
            : new Refusal("The AI service could not be reached.", status);
    }
}

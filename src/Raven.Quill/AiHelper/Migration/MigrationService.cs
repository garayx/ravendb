using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
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

        // Every session persists its plan at the end of its opening turn, so nothing found here means
        // the conversation is not this app's to continue, whether or not it exists at all.
        if (await client.GetAsync(request.Slug, request.ConversationId, token) is null)
            return new Refusal("no planning session found for that conversation");

        await client.AskAsync(
            new MigrationAskCommand(request.Slug, request.ConversationId, schema!, request.Prompt), onFrame, token);

        return null;
    }

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

        var entries = await client.GetAsync(request.Slug, request.ConversationId, token);
        if (entries is null)
            return (null, new Refusal("no plan found for that conversation"));

        if (entries.Count == 0)
            return (null, new Refusal("the plan has no collections yet"));

        var (selected, unknown) = SelectCollections(entries, request.Collections);

        if (unknown.Length > 0)
            return (null, new Refusal($"the plan has no collection named {string.Join(", ", unknown)}"));

        if (selected.Length == 0)
            return (null, new Refusal("no collections were selected"));

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);

        if (state?.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        var configuration = PlanToCdcConfiguration.Build(selected, state.LastMapConfiguration);

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
    private static (PlanEntry[] Selected, string[] Unknown) SelectCollections(
        IReadOnlyCollection<PlanEntry> entries,
        string[]? requested)
    {
        if (requested is not { Length: > 0 })
            return (entries.ToArray(), []);

        var wanted = requested.ToHashSet(StringComparer.OrdinalIgnoreCase);

        var unknown = wanted
            .Where(name => entries.Any(e => string.Equals(e.Collection, name, StringComparison.OrdinalIgnoreCase)) == false)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        return (entries.Where(e => wanted.Contains(e.Collection)).ToArray(), unknown);
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

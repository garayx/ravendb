using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.Contracts;

/// <summary>Begins a planning session over the schema the wizard already discovered for this app.</summary>
public sealed record MigrationStartRequest(
    string Slug,
    SelectedSourceTable[]? SelectedTables = null,
    string? Prompt = null);

/// <summary>One more turn in an existing planning session.</summary>
public sealed record MigrationAskRequest(string Slug, string ConversationId, string Prompt);

/// <summary>Branches off a stored analysis, so the same schema is not analysed twice.</summary>
public sealed record MigrationForkRequest(string Slug, string InputKey, string Branch);

/// <summary>
/// Turns what the session registered into the configuration the wizard carries on with.
/// <paramref name="Collections"/> narrows it to the ones the operator kept; empty or absent takes
/// the whole plan. Anything left out is reported back in the coverage diff, not silently dropped.
/// </summary>
public sealed record MigrationApplyRequest(string Slug, string ConversationId, string[]? Collections = null);

public sealed record MigrationApplyResponse(
    CdcSinkConfiguration? Configuration,
    string[] UnmappedTables,
    string[] Errors);

public sealed record MigrationPlanSnapshot(
    string ConversationId,
    string Slug,
    string? InputKey,
    PropertyCase PropertyCase,
    string? PropertyLanguage,
    MigrationPlanCollection[] Collections,
    string[] Prompts);

public sealed record MigrationPlanCollection(
    string Collection,
    int Version,
    string? Rationale,
    CdcSinkTableConfig? Config);

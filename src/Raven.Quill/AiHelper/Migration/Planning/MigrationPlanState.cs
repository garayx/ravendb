namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// The persisted form of a <see cref="MigrationPlan"/>. A planning session outlives the request
/// that drove it, so what the model registered has to survive a page reload rather than living in
/// the memory of whichever request happened to be running the turn.
/// </summary>
public sealed class MigrationPlanState
{
    public const string Collection = "@migration-plans";

    public string Id { get; set; } = string.Empty;

    public string ConversationId { get; set; } = string.Empty;

    /// <summary>The app this planning session belongs to. A conversation is never shared between apps.</summary>
    public string Slug { get; set; } = string.Empty;

    public string? InputKey { get; set; }

    public NamingConventions Conventions { get; set; } = NamingConventions.None;

    public string? ProposalJson { get; set; }

    public List<PlanEntry> Entries { get; set; } = [];

    public List<string> Prompts { get; set; } = [];

    public DateTime CreatedAt { get; set; }

    public DateTime UpdatedAt { get; set; }

    public static string DocumentId(string conversationId) => $"{Collection}/{conversationId}";
}

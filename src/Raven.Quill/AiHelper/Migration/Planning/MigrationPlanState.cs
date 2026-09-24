namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// What the model registered in a conversation, kept between the requests of one wizard session:
/// every turn and the final apply are separate calls, and each needs the plan as it stands.
/// </summary>
public sealed class MigrationPlanState
{
    public const string Collection = "@migration-plans";

    public string Id { get; set; } = string.Empty;

    /// <summary>The app this planning session belongs to. A conversation is never shared between apps.</summary>
    public string Slug { get; set; } = string.Empty;

    // A fresh instance per document: the store fills an existing value in place when loading, so a
    // shared default would carry one plan's conventions into every plan loaded after it.
    public NamingConventions Conventions { get; set; } = new();

    public List<PlanEntry> Entries { get; set; } = [];

    public static string DocumentId(string conversationId) => $"{Collection}/{conversationId}";
}

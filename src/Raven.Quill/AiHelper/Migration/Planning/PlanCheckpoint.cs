using System.Security.Cryptography;
using System.Text;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// The analysis of a schema, keyed by everything that could change it. Lets a second run over the
/// same schema and the same prompts skip straight to the proposal instead of paying for it again.
/// </summary>
public sealed class PlanCheckpoint
{
    public const string Collection = "@migration-checkpoints";

    public string Id { get; set; } = string.Empty;

    public string InputKey { get; set; } = string.Empty;

    public string AgentIdentifier { get; set; } = string.Empty;

    /// <summary>
    /// The app whose schema this analysis was taken of. A fork copies the checkpoint's DDL and its
    /// proposal into a new conversation, so it has to be refused when it belongs to another app.
    /// </summary>
    public string Slug { get; set; } = string.Empty;

    public string? SourceConversationId { get; set; }

    public DateTime CreatedAt { get; set; }

    public List<SchemaFile> Schema { get; set; } = [];

    public List<string> Prompts { get; set; } = [];

    public string? ProposalJson { get; set; }

    public static string DocumentId(string inputKey) => $"{Collection}/{inputKey}";

    public string ConversationIdFor(string branch) => $"MigrationChats/{InputKey}/{branch}";

    /// <summary>
    /// Deterministic: the same agent, schema files and prompts produce the same key on any machine.
    /// Changing any of them invalidates the checkpoint rather than resuming against it.
    /// </summary>
    public static string ComputeInputKey(
        string agentIdentifier,
        IEnumerable<SchemaFile> files,
        IEnumerable<string> prompts)
    {
        var sb = new StringBuilder()
            .Append(agentIdentifier).Append('\n');

        foreach (var file in files.OrderBy(f => f.Name, StringComparer.Ordinal))
            sb.Append(file.Name).Append('\t').Append(file.Digest).Append('\n');

        sb.Append('\n');

        foreach (var prompt in prompts)
            sb.Append(prompt).Append('\n');

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
    }
}

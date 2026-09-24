using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// The planning agent, behind the shape its HTTP contract will have.
///
/// <see cref="LocalMigrationClient"/> drives the conversation in this process, against the agent
/// registered in Quill's own database - one project to run while this is being built. When the
/// agent moves to api.ravendb.net a second implementation posts these same commands to
/// /assistant/migration/*, exactly as <see cref="AiHelperInternalClient"/> posts to
/// /assistant/assist today, and nothing above this interface changes.
/// </summary>
public interface IMigrationClient
{
    Task StartAsync(MigrationStartCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token);

    Task AskAsync(MigrationAskCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token);

    /// <summary>What a conversation has registered, or null when no such plan belongs to <paramref name="slug"/>.</summary>
    Task<IReadOnlyCollection<PlanEntry>?> GetAsync(string slug, string conversationId, CancellationToken token);
}

public sealed record MigrationStartCommand(
    string Slug,
    CdcSinkSourceSchema Schema,
    string Prompt);

public sealed record MigrationAskCommand(
    string Slug,
    string ConversationId,
    CdcSinkSourceSchema Schema,
    string Prompt);

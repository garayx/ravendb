using Raven.Client.Documents;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// Puts the planner agent in the database Quill drives conversations against, and keeps it current.
///
/// The check is against the deployed system prompt rather than a recorded version number, because a
/// prompt edit that forgot to bump <see cref="SchemaMigrationAgentDefinition.SystemPromptVersion"/>
/// is exactly the case a version comparison would miss and a text comparison catches.
/// </summary>
public sealed class MigrationAgentInstaller(IDocumentStore store)
{
    /// <summary>Returns true when the agent was written, false when it was already current.</summary>
    public async Task<bool> EnsureRegisteredAsync(string connectionStringName, CancellationToken token = default)
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
        {
            throw new InvalidOperationException(
                "The schema migration planner needs an AI connection string to run against. " +
                "Configure one before starting a migration session.");
        }

        var existing = await store.AI.GetAgentAsync(SchemaMigrationAgentDefinition.Identifier, token);

        if (existing is not null &&
            string.Equals(existing.SystemPrompt, SchemaMigrationAgentDefinition.SystemPrompt, StringComparison.Ordinal) &&
            string.Equals(existing.ConnectionStringName, connectionStringName, StringComparison.Ordinal) &&
            existing.Disabled == false)
        {
            return false;
        }

        await SchemaMigrationAgentDefinition.CreateOrUpdateAsync(store, connectionStringName, token);
        return true;
    }
}

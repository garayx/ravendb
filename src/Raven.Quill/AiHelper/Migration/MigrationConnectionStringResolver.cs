using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide.Operations.ConnectionStrings;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// Picks the AI connection string the planner runs against. Quill's AI connection strings are
/// server-wide, so one is already reachable from the config database the planner lives in - there
/// is no separate appliance-level setting to provision.
/// </summary>
public sealed class MigrationConnectionStringResolver(IDocumentStore store)
{
    public async Task<string> ResolveAsync(string? requested, CancellationToken token = default)
    {
        var chat = await ChatConnectionStringsAsync(token);

        if (string.IsNullOrWhiteSpace(requested) == false)
        {
            var match = chat.FirstOrDefault(c => string.Equals(c, requested, StringComparison.OrdinalIgnoreCase));

            return match ?? throw new InvalidOperationException(
                $"AI connection string '{requested}' was not found. Available: " +
                $"{(chat.Count == 0 ? "none" : string.Join(", ", chat))}.");
        }

        return chat.Count switch
        {
            1 => chat[0],
            0 => throw new InvalidOperationException(
                "No AI connection string is configured. Create one at /api/ai/connection-strings before " +
                "starting a migration session."),
            _ => throw new InvalidOperationException(
                $"More than one AI connection string is configured ({string.Join(", ", chat)}); " +
                "name the one to use in aiConnectionStringName.")
        };
    }

    private async Task<List<string>> ChatConnectionStringsAsync(CancellationToken token)
    {
        var all = await store.Maintenance.Server.SendAsync(new GetServerWideConnectionStringsOperation(), token);

        return all.Results
            .Select(c => c.ConnectionString)
            .OfType<AiConnectionString>()
            .Where(c => c.ModelType == AiModelType.Chat)
            .Select(c => c.Name)
            .Where(n => string.IsNullOrWhiteSpace(n) == false)
            .ToList();
    }
}

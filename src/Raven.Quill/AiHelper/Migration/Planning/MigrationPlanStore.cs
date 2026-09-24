using Raven.Client.Documents;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// Reads and writes the plan behind a conversation. Written after every accepted tool call rather
/// than once at the end of a turn: by the time a collection is registered the user has already been
/// shown it, so a turn that dies half way through must not take it back.
/// </summary>
public sealed class MigrationPlanStore(IDocumentStore store)
{
    public async Task<MigrationPlanState?> LoadAsync(string conversationId, CancellationToken token = default)
    {
        using var session = store.OpenAsyncSession();
        return await session.LoadAsync<MigrationPlanState>(MigrationPlanState.DocumentId(conversationId), token);
    }

    public async Task SaveAsync(string slug, string conversationId, MigrationPlan plan, CancellationToken token = default)
    {
        var state = new MigrationPlanState
        {
            Id = MigrationPlanState.DocumentId(conversationId),
            Slug = slug,
            Conventions = plan.Conventions,
            Entries = plan.Entries.ToList()
        };

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(state, state.Id, token);
        await session.SaveChangesAsync(token);
    }
}

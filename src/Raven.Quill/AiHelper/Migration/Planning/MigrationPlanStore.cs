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

    public async Task SaveAsync(
        string slug,
        string conversationId,
        MigrationPlan plan,
        string? inputKey,
        IEnumerable<string> prompts,
        CancellationToken token = default)
    {
        var id = MigrationPlanState.DocumentId(conversationId);

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<MigrationPlanState>(id, token);

        if (state is null)
        {
            state = new MigrationPlanState
            {
                Id = id,
                ConversationId = conversationId,
                Slug = slug,
                CreatedAt = DateTime.UtcNow
            };

            await session.StoreAsync(state, id, token);
        }

        state.Slug = slug;
        state.InputKey = inputKey;
        state.Conventions = plan.Conventions;
        state.ProposalJson = plan.Proposal is { } proposal ? proposal.GetRawText() : null;
        state.Entries = plan.Entries.ToList();
        state.Prompts = prompts.ToList();
        state.UpdatedAt = DateTime.UtcNow;

        await session.SaveChangesAsync(token);
    }
}

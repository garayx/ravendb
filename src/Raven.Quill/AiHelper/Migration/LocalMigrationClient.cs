using System.Threading.Channels;
using Raven.Client.Documents;
using Raven.Quill.AiHelper.Migration.Agent;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// Drives the planning conversation in this process. The tool handlers have to run where Quill can
/// validate what the model emitted and reject it back into the same turn, which is only possible
/// while Quill is the one calling RunAsync.
/// </summary>
public sealed class LocalMigrationClient(
    IDocumentStore store,
    MigrationPlanStore plans,
    MigrationAgentInstaller installer) : IMigrationClient
{
    public Task StartAsync(MigrationStartCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(command.AiConnectionStringName, onFrame, token, async (channel, _) =>
        {
            var catalog = SchemaCatalog.FromDiscoveredSchema(command.Schema);

            var session = MigrationSession.Start(
                store, plans, channel, command.Slug, catalog,
                maxModelIterationsPerCall: MigrationSession.IterationBudgetFor(catalog.Files.Count));

            session.AttachSchema();

            var reply = await session.AskAsync(command.Prompt, token);

            // The analysis is the expensive part, so it is checkpointed the moment it exists: a
            // second run over the same schema and prompts forks from here instead of paying again.
            await session.CheckpointAsync(token);

            return (session, reply);
        });

    public Task AskAsync(MigrationAskCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(command.AiConnectionStringName, onFrame, token, async (channel, _) =>
        {
            var catalog = SchemaCatalog.FromDiscoveredSchema(command.Schema);

            var session = await MigrationSession.ResumeAsync(
                store, plans, channel, command.Slug, catalog, command.ConversationId,
                MigrationSession.IterationBudgetFor(catalog.Files.Count), token);

            var reply = await session.AskAsync(command.Prompt, token);
            return (session, reply);
        });

    public Task ForkAsync(MigrationForkCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(command.AiConnectionStringName, onFrame, token, async (channel, _) =>
        {
            var checkpoint = await MigrationSession.FindCheckpointAsync(store, command.InputKey, token)
                ?? throw new InvalidOperationException($"No stored analysis found for input key '{command.InputKey}'.");

            var session = MigrationSession.Fork(
                store, plans, channel, command.Slug, checkpoint, command.Branch,
                MigrationSession.IterationBudgetFor(checkpoint.Schema.Count));

            return (session, new MigrationReply());
        });

    public async Task<MigrationPlanSnapshot?> GetAsync(string conversationId, CancellationToken token)
    {
        var state = await plans.LoadAsync(conversationId, token);

        if (state is null)
            return null;

        return new MigrationPlanSnapshot(
            state.ConversationId,
            state.Slug,
            state.InputKey,
            state.Conventions?.PropertyCase ?? PropertyCase.Unspecified,
            state.Conventions?.PropertyLanguage,
            state.Entries
                .Select(e => new MigrationPlanCollection(e.Collection, e.Version, e.Rationale, e.Config))
                .ToArray(),
            state.Prompts.ToArray());
    }

    /// <summary>
    /// Runs the turn on one task while the caller drains the frames it produces, so collections
    /// reach the browser as they are registered rather than in one batch at the end.
    /// </summary>
    private async Task StreamAsync(
        string aiConnectionStringName,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token,
        Func<IPlanChannel, CancellationToken, Task<(MigrationSession Session, MigrationReply Reply)>> run)
    {
        await installer.EnsureRegisteredAsync(aiConnectionStringName, token);

        var queue = Channel.CreateUnbounded<MigrationFrame>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var worker = Task.Run(async () =>
        {
            try
            {
                var (session, reply) = await run(new QueuedPlanChannel(queue.Writer), token);

                queue.Writer.TryWrite(new ReplyFrame
                {
                    Reply = reply.Reply,
                    Gaps = reply.Gaps,
                    OpenQuestions = reply.OpenQuestions
                });

                queue.Writer.TryWrite(new DoneFrame
                {
                    ConversationId = session.ConversationId,
                    InputKey = session.InputKey()
                });
            }
            catch (OperationCanceledException)
            {
                // The caller went away; there is nobody left to tell.
            }
            catch (Exception e)
            {
                queue.Writer.TryWrite(new ErrorFrame { Message = e.Message });
            }
            finally
            {
                queue.Writer.TryComplete();
            }
        }, token);

        await foreach (var frame in queue.Reader.ReadAllAsync(token))
            await onFrame(frame);

        await worker;
    }
}

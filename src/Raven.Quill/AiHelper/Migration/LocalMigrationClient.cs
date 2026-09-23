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
public sealed class LocalMigrationClient(IDocumentStore store, MigrationPlanStore plans) : IMigrationClient
{
    public Task StartAsync(MigrationStartCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(onFrame, async channel =>
        {
            var session = MigrationSession.Start(
                store, plans, channel, command.Slug, SchemaCatalog.FromDiscoveredSchema(command.Schema));

            session.AttachSchema();

            var reply = await session.AskAsync(command.Prompt, token);

            // The analysis is the expensive part, so it is checkpointed the moment it exists: a
            // second run over the same schema and prompts forks from here instead of paying again.
            await session.CheckpointAsync(token);

            return (session, reply);
        });

    public Task AskAsync(MigrationAskCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(onFrame, async channel =>
        {
            var session = await MigrationSession.ResumeAsync(
                store, plans, channel, command.Slug, SchemaCatalog.FromDiscoveredSchema(command.Schema),
                command.ConversationId, token);

            var reply = await session.AskAsync(command.Prompt, token);
            return (session, reply);
        });

    public Task ForkAsync(MigrationForkCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(onFrame, async channel =>
        {
            var checkpoint = await MigrationSession.FindCheckpointAsync(store, command.InputKey, token)
                ?? throw new InvalidOperationException($"No stored analysis found for input key '{command.InputKey}'.");

            // The discovered schema is exact, so the branch validates against the same facts the
            // original session did rather than against DDL parsed back off the checkpoint.
            var session = MigrationSession.Fork(
                store, plans, channel, command.Slug, checkpoint, command.Branch,
                SchemaCatalog.FromDiscoveredSchema(command.Schema));

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
    private static async Task StreamAsync(
        Func<MigrationFrame, Task> onFrame,
        Func<IPlanChannel, Task<(MigrationSession Session, MigrationReply Reply)>> run)
    {
        var queue = Channel.CreateUnbounded<MigrationFrame>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var worker = Task.Run(async () =>
        {
            try
            {
                var (session, reply) = await run(new QueuedPlanChannel(queue.Writer));

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
                // The whole exception, not just the message: this is an operator-facing setup tool
                // and the inner exception is usually the only thing that says what actually broke.
                queue.Writer.TryWrite(new ErrorFrame { Message = e.ToString() });
            }
            finally
            {
                queue.Writer.TryComplete();
            }
        });

        await foreach (var frame in queue.Reader.ReadAllAsync())
            await onFrame(frame);

        await worker;
    }
}

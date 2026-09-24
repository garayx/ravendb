using System.Threading.Channels;
using Raven.Client.Documents;
using Raven.Quill.AiHelper.Migration.Agent;
using Raven.Quill.AiHelper.Migration.Planning;

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

            return (session, await session.AskAsync(command.Prompt, token));
        });

    public Task AskAsync(MigrationAskCommand command, Func<MigrationFrame, Task> onFrame, CancellationToken token) =>
        StreamAsync(onFrame, async channel =>
        {
            var session = await MigrationSession.ContinueAsync(
                store, plans, channel, command.Slug, SchemaCatalog.FromDiscoveredSchema(command.Schema),
                command.ConversationId, token);

            return (session, await session.AskAsync(command.Prompt, token));
        });

    public async Task<IReadOnlyCollection<PlanEntry>?> GetAsync(string slug, string conversationId, CancellationToken token)
    {
        var state = await plans.LoadAsync(conversationId, token);

        // Plans for every app share one database, so a conversation id alone is not an entitlement
        // to read one. A mismatch reads as "no such plan" rather than admitting the plan exists.
        return state is not null && string.Equals(state.Slug, slug, StringComparison.OrdinalIgnoreCase)
            ? state.Entries
            : null;
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
                    OpenQuestions = OpenQuestion.From(reply.OpenQuestions)
                });

                queue.Writer.TryWrite(new DoneFrame { ConversationId = session.ConversationId });
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

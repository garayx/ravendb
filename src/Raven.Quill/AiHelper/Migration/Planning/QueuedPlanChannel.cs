using System.Threading.Channels;
using Raven.Quill.AiHelper.Migration.Agent;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// Turns plan changes into stream frames. The tool handlers are synchronous and run inside the
/// conversation turn, so they hand frames to a queue rather than writing to the response - the
/// request drains that queue and does the writing.
/// </summary>
public sealed class QueuedPlanChannel(ChannelWriter<MigrationFrame> writer) : IPlanChannel
{
    public void ProposalRegistered(ProposePlanArgs proposal) =>
        writer.TryWrite(new ProposalFrame
        {
            Areas = proposal.Areas,
            Collections = proposal.Collections,
            Dropped = proposal.Dropped,
            Enables = proposal.Enables
        });

    public void CollectionRegistered(PlanEntry entry, List<string> warnings) =>
        writer.TryWrite(new CollectionFrame
        {
            Status = entry.Version == 1 ? "registered" : "replaced",
            Collection = entry.Collection,
            Version = entry.Version,
            Rationale = entry.Rationale,
            Config = entry.Config,
            Warnings = warnings.ToArray()
        });

    public void CollectionRejected(string collection, List<string> errors) =>
        writer.TryWrite(new RejectedFrame
        {
            Collection = collection,
            Errors = errors.ToArray()
        });

    public void CollectionRemoved(string? collection, string? reason) =>
        writer.TryWrite(new RemovedFrame
        {
            Collection = collection,
            Reason = reason
        });

    public void ConventionsChanged(NamingConventions conventions, string[] mustReEmit) =>
        writer.TryWrite(new ConventionsFrame
        {
            PropertyCase = conventions.PropertyCase,
            PropertyLanguage = conventions.PropertyLanguage,
            Notes = conventions.Notes,
            MustReEmit = mustReEmit
        });

    public void Note(string text) => writer.TryWrite(new NoteFrame { Text = text });
}

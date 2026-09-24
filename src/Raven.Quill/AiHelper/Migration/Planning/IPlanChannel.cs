using Raven.Quill.AiHelper.Migration.Agent;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// Where plan changes go as they happen. Every notification corresponds to a tool call that already
/// succeeded or already failed, so the user sees the same outcome the model was told about.
/// </summary>
public interface IPlanChannel
{
    void ProposalRegistered(ProposePlanArgs proposal);

    void CollectionRegistered(PlanEntry entry, List<string> warnings);

    void CollectionRejected(string collection, List<string> errors);

    void CollectionRemoved(string? collection, string? reason);

    void ConventionsChanged(NamingConventions conventions, string[] mustReEmit);

    void Note(string text);
}

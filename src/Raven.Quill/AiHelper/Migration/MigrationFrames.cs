using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Agent;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// One line of the NDJSON stream. Every frame corresponds to something that already happened - a
/// tool call that succeeded or failed - so the browser and the model are told the same story.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(ProposalFrame), "proposal")]
[JsonDerivedType(typeof(CollectionFrame), "collection")]
[JsonDerivedType(typeof(RejectedFrame), "rejected")]
[JsonDerivedType(typeof(RemovedFrame), "removed")]
[JsonDerivedType(typeof(ConventionsFrame), "conventions")]
[JsonDerivedType(typeof(NoteFrame), "note")]
[JsonDerivedType(typeof(ReplyFrame), "reply")]
[JsonDerivedType(typeof(DoneFrame), "done")]
[JsonDerivedType(typeof(ErrorFrame), "error")]
public abstract class MigrationFrame;

public sealed class ProposalFrame : MigrationFrame
{
    public ProposedArea[] Areas { get; set; } = [];
    public ProposedCollection[] Collections { get; set; } = [];
    public DroppedTable[] Dropped { get; set; } = [];
    public string[] Enables { get; set; } = [];
}

public sealed class CollectionFrame : MigrationFrame
{
    public string Status { get; set; } = string.Empty;
    public string Collection { get; set; } = string.Empty;
    public int Version { get; set; }
    public string? Rationale { get; set; }
    public CdcSinkTableConfig? Config { get; set; }
    public string[] Warnings { get; set; } = [];
}

public sealed class RejectedFrame : MigrationFrame
{
    public string Collection { get; set; } = string.Empty;
    public string[] Errors { get; set; } = [];
}

public sealed class RemovedFrame : MigrationFrame
{
    public string? Collection { get; set; }
    public string? Reason { get; set; }
}

public sealed class ConventionsFrame : MigrationFrame
{
    public PropertyCase PropertyCase { get; set; }
    public string? PropertyLanguage { get; set; }
    public string? Notes { get; set; }
    public string[] MustReEmit { get; set; } = [];
}

public sealed class NoteFrame : MigrationFrame
{
    public string Text { get; set; } = string.Empty;
}

public sealed class ReplyFrame : MigrationFrame
{
    public string? Reply { get; set; }
    public string[] Gaps { get; set; } = [];
    public OpenQuestion[] OpenQuestions { get; set; } = [];
}

/// <summary>
/// A question as the browser renders it. <see cref="Recommended"/> indexes into
/// <see cref="Options"/>, and is null only when the agent offered no answers at all.
/// </summary>
public sealed record OpenQuestion(string Question, string[] Options, int? Recommended)
{
    public const int MaxOptions = 3;

    /// <summary>
    /// The model is asked for three answers with exactly one recommended, and does not always
    /// comply. Blank questions and answers are dropped, extra answers are cut, and when no kept
    /// answer is flagged the first one stands in, so the browser can always resolve a skip.
    /// </summary>
    public static OpenQuestion[] From(MigrationOpenQuestion[]? questions) =>
        (questions ?? [])
            .Where(q => string.IsNullOrWhiteSpace(q.Question) == false)
            .Select(q =>
            {
                var options = (q.Options ?? [])
                    .Where(o => string.IsNullOrWhiteSpace(o.Answer) == false)
                    .DistinctBy(o => o.Answer!.Trim(), StringComparer.OrdinalIgnoreCase)
                    .Take(MaxOptions)
                    .ToArray();

                var recommended = Array.FindIndex(options, o => o.IsRecommended);

                return new OpenQuestion(
                    q.Question!.Trim(),
                    options.Select(o => o.Answer!.Trim()).ToArray(),
                    options.Length == 0 ? null : Math.Max(recommended, 0));
            })
            .ToArray();
}

public sealed class DoneFrame : MigrationFrame
{
    public string ConversationId { get; set; } = string.Empty;
    public string? InputKey { get; set; }
}

public sealed class ErrorFrame : MigrationFrame
{
    public string Message { get; set; } = string.Empty;
}

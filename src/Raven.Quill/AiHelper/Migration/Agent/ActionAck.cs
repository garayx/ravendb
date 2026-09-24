namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// What a tool call returns to the model. <see cref="Next"/> is steering, not logging: it is the
/// cheapest place to tell the model what to do with the outcome it just got.
/// </summary>
public sealed class ActionAck
{
    public string? Status { get; set; }

    public string? Collection { get; set; }

    public int? Version { get; set; }

    public string[]? Errors { get; set; }

    public string[]? Warnings { get; set; }

    public string[]? Registered { get; set; }

    public string? Next { get; set; }

    public static ActionAck Rejected(string collection, IEnumerable<string> errors, IEnumerable<string> warnings) =>
        new()
        {
            Status = "rejected",
            Collection = collection,
            Errors = errors.ToArray(),
            Warnings = warnings.ToArray(),
            Next = "Nothing was registered. Fix every error listed above and call add_collection again " +
                   "for this collection. Do not describe it as done and do not work around the errors in prose."
        };
}

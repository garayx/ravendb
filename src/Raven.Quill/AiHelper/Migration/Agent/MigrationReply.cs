namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>The agent's written answer. The configuration itself travels through the tools.</summary>
public sealed class MigrationReply
{
    public string? Reply { get; set; }

    public string[] Gaps { get; set; } = [];

    public string[] OpenQuestions { get; set; } = [];
}

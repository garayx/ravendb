namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>The agent's written answer. The configuration itself travels through the tools.</summary>
public sealed class MigrationReply
{
    public string? Reply { get; set; }

    public string[] Gaps { get; set; } = [];

    public MigrationOpenQuestion[] OpenQuestions { get; set; } = [];
}

/// <summary>A decision the agent wants the user to make, offered as answers they can pick without typing.</summary>
public sealed class MigrationOpenQuestion
{
    public string? Question { get; set; }

    public MigrationAnswerOption[] Options { get; set; } = [];
}

public sealed class MigrationAnswerOption
{
    public string? Answer { get; set; }

    public bool IsRecommended { get; set; }
}

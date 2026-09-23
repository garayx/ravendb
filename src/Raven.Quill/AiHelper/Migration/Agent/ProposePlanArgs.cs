namespace Raven.Quill.AiHelper.Migration.Agent;

public sealed class ProposePlanArgs
{
    public ProposedArea[] Areas { get; set; } = [];

    public ProposedCollection[] Collections { get; set; } = [];

    public DroppedTable[] Dropped { get; set; } = [];

    public string[] Enables { get; set; } = [];
}

public sealed class ProposedArea
{
    public string? Area { get; set; }

    public string[] Collections { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class ProposedCollection
{
    public string? Collection { get; set; }

    public string? RootTable { get; set; }

    public AbsorbedTable[] Absorbs { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class AbsorbedTable
{
    public string? Table { get; set; }

    /// <summary>"Embed" or "Link".</summary>
    public string? How { get; set; }

    public string? Why { get; set; }
}

public sealed class DroppedTable
{
    public string? Table { get; set; }

    public string? Why { get; set; }
}

namespace Raven.Quill.AiHelper.Migration.Planning;

public sealed record TableUse(string Collection, TableUseKind Kind)
{
    public override string ToString() => Kind switch
    {
        TableUseKind.Root => $"the root of {Collection}",
        TableUseKind.Embedded => $"embedded in {Collection}",
        TableUseKind.Linked => $"linked from {Collection}",
        _ => $"used by {Collection}"
    };
}

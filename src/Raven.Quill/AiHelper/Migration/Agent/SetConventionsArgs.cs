using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration.Agent;

public sealed class SetConventionsArgs
{
    /// <summary>Kept as text so a spelling the enum does not use is answered, not thrown on.</summary>
    public string? PropertyCase { get; set; }

    public string? PropertyLanguage { get; set; }

    public string? Notes { get; set; }
}

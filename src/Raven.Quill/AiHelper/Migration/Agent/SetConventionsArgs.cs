using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration.Agent;

public sealed class SetConventionsArgs
{
    public PropertyCase PropertyCase { get; set; }

    public string? PropertyLanguage { get; set; }

    public string? Notes { get; set; }
}

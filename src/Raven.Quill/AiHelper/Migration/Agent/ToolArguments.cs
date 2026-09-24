using Newtonsoft.Json;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// Reads a tool call's arguments. The conversation client converts arguments before its own error
/// handling starts, so a value the model got wrong - an enum member that does not exist - would end
/// the turn instead of being sent back. Converting here keeps it a mistake the model can correct.
/// </summary>
public static class ToolArguments
{
    public static bool TryRead<TArgs>(string? raw, out TArgs args, out string error) where TArgs : class
    {
        args = null!;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(raw))
        {
            error = "The call had no arguments.";
            return false;
        }

        try
        {
            args = JsonConvert.DeserializeObject<TArgs>(raw)!;
        }
        catch (JsonException e)
        {
            error = e.Message;
            return false;
        }

        if (args is not null)
            return true;

        error = "The call had no arguments.";
        return false;
    }
}

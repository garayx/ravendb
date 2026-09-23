using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.AiHelper.Migration.Planning;

public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    private static readonly JsonSerializerOptions PrettyOptions = new(Options)
    {
        WriteIndented = true
    };

    public static string Pretty(object value) => JsonSerializer.Serialize(value, PrettyOptions);
}

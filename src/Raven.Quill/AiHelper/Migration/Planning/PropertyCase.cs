namespace Raven.Quill.AiHelper.Migration.Planning;

public enum PropertyCase
{
    Unspecified,
    SnakeCase,
    CamelCase,
    PascalCase
}

public static class PropertyCases
{
    public static readonly string[] Names = [nameof(PropertyCase.SnakeCase), nameof(PropertyCase.CamelCase), nameof(PropertyCase.PascalCase)];

    /// <summary>
    /// The model names a case the way people write it - "snake_case", "camel case", "Pascal" - not
    /// the way the enum spells it, so separators, a trailing "case" and letter case are ignored.
    /// </summary>
    public static bool TryParse(string? value, out PropertyCase propertyCase)
    {
        propertyCase = PropertyCase.Unspecified;

        if (string.IsNullOrWhiteSpace(value))
            return false;

        var key = new string(value.Where(char.IsLetter).ToArray()).ToLowerInvariant();
        if (key.EndsWith("case", StringComparison.Ordinal))
            key = key[..^"case".Length];

        propertyCase = key switch
        {
            "snake" => PropertyCase.SnakeCase,
            "camel" => PropertyCase.CamelCase,
            "pascal" => PropertyCase.PascalCase,
            _ => PropertyCase.Unspecified
        };

        return propertyCase != PropertyCase.Unspecified;
    }
}

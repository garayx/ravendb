using System.Text.RegularExpressions;

namespace Raven.Quill.AiHelper.Migration.Schema;

/// <summary>
/// Not a SQL parser. It answers three questions and nothing else: does this DDL declare table T,
/// which columns does it declare, and is column C among them. Anything it cannot read is simply
/// not reported, which makes the validator's column check skip rather than reject a column it
/// only failed to parse.
/// </summary>
public static class DdlColumnExtractor
{
    private const string Ident = @"(?:\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][\w$]*)";

    private static readonly Regex CreateTable = new(
        $@"CREATE\s+(?:GLOBAL\s+|LOCAL\s+)?(?:TEMP(?:ORARY)?\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<name>{Ident}(?:\s*\.\s*{Ident})*)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly string[] ConstraintLeaders =
    [
        "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK", "KEY", "INDEX", "EXCLUDE", "PERIOD", "LIKE", "INHERITS"
    ];

    public readonly record struct DeclaredTable(string Schema, string Name, IReadOnlyList<string> Columns);

    public static IEnumerable<DeclaredTable> Extract(string ddl)
    {
        foreach (Match match in CreateTable.Matches(ddl))
        {
            var body = ReadBalancedBody(ddl, match.Index + match.Length - 1);
            if (body is null)
                continue;

            var (schema, name) = SplitQualified(match.Groups["name"].Value);

            yield return new DeclaredTable(schema, name, ColumnNames(body).ToArray());
        }
    }

    private static IEnumerable<string> ColumnNames(string body)
    {
        foreach (var item in SplitTopLevel(body))
        {
            var (name, quoted) = LeadingIdentifier(item);

            if (string.IsNullOrEmpty(name))
                continue;

            // A quoted identifier is a column even when it spells a keyword, so only an unquoted
            // leader can mark the item as a table-level constraint.
            if (quoted == false && ConstraintLeaders.Contains(name, StringComparer.OrdinalIgnoreCase))
                continue;

            yield return name;
        }
    }

    /// <summary>
    /// The first identifier of a column definition. Quoted forms may contain spaces, so this cannot
    /// simply split on whitespace.
    /// </summary>
    private static (string Name, bool Quoted) LeadingIdentifier(string item)
    {
        var s = item.TrimStart();

        if (s.Length == 0)
            return (string.Empty, false);

        char? closing = s[0] switch
        {
            '[' => ']',
            '"' => '"',
            '`' => '`',
            _ => null
        };

        if (closing is not null)
        {
            var end = s.IndexOf(closing.Value, 1);
            return end < 0 ? (string.Empty, false) : (s[1..end].Trim(), true);
        }

        var i = 0;
        while (i < s.Length && char.IsWhiteSpace(s[i]) == false && s[i] != '(' && s[i] != ',')
            i++;

        return (s[..i], false);
    }

    private static IEnumerable<string> SplitTopLevel(string body)
    {
        var depth = 0;
        var start = 0;
        char? closing = null;

        for (var i = 0; i < body.Length; i++)
        {
            var c = body[i];

            if (closing is not null)
            {
                if (c == closing)
                    closing = null;
                continue;
            }

            switch (c)
            {
                case '\'' or '"' or '`':
                    closing = c;
                    break;
                case '[':
                    closing = ']';
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',' when depth == 0:
                    yield return body[start..i];
                    start = i + 1;
                    break;
            }
        }

        if (start < body.Length)
            yield return body[start..];
    }

    private static string? ReadBalancedBody(string ddl, int openParenIndex)
    {
        var depth = 0;
        char? closing = null;

        for (var i = openParenIndex; i < ddl.Length; i++)
        {
            var c = ddl[i];

            if (closing is not null)
            {
                if (c == closing)
                    closing = null;
                continue;
            }

            switch (c)
            {
                case '\'' or '"' or '`':
                    closing = c;
                    break;
                case '[':
                    closing = ']';
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                        return ddl[(openParenIndex + 1)..i];
                    break;
            }
        }

        return null;
    }

    private static (string Schema, string Name) SplitQualified(string qualified)
    {
        var parts = SplitOnDots(qualified).Select(Unquote).Where(p => p.Length > 0).ToArray();

        return parts.Length switch
        {
            0 => (string.Empty, string.Empty),
            1 => (string.Empty, parts[0]),
            _ => (parts[^2], parts[^1])
        };
    }

    private static IEnumerable<string> SplitOnDots(string value)
    {
        var start = 0;
        char? closing = null;

        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];

            if (closing is not null)
            {
                if (c == closing)
                    closing = null;
                continue;
            }

            switch (c)
            {
                case '"' or '`':
                    closing = c;
                    break;
                case '[':
                    closing = ']';
                    break;
                case '.':
                    yield return value[start..i];
                    start = i + 1;
                    break;
            }
        }

        if (start <= value.Length - 1)
            yield return value[start..];
    }

    private static string Unquote(string value)
    {
        var trimmed = value.Trim();

        if (trimmed.Length < 2)
            return trimmed;

        var first = trimmed[0];
        var last = trimmed[^1];

        if ((first == '[' && last == ']') || (first == '"' && last == '"') || (first == '`' && last == '`'))
            return trimmed[1..^1].Trim();

        return trimmed;
    }
}

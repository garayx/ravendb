using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// The table and column facts the validator holds the model to. Built from DDL files on disk;
/// a catalog that could not parse a table answers <see cref="Knows"/> with false, which makes the
/// column check skip rather than reject a column it simply failed to read.
/// </summary>
public sealed class SchemaCatalog
{
    private static readonly Regex CreateTable = new(
        @"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<name>(?:\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][\w$]*))*)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly string[] ConstraintLeaders =
    [
        "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK", "KEY", "INDEX", "EXCLUDE", "PERIOD"
    ];

    private readonly Dictionary<string, HashSet<string>> _columnsByTable =
        new(StringComparer.OrdinalIgnoreCase);

    private SchemaCatalog(List<SchemaFile> files)
    {
        Files = files;
    }

    public List<SchemaFile> Files { get; }

    public static SchemaCatalog FromFiles(IEnumerable<string> ddlPaths)
    {
        var files = new List<SchemaFile>();
        var catalog = new SchemaCatalog(files);

        foreach (var path in ddlPaths)
        {
            var content = File.ReadAllText(path);

            files.Add(new SchemaFile
            {
                Name = System.IO.Path.GetFileName(path),
                Path = System.IO.Path.GetFullPath(path),
                Digest = Digest(content)
            });

            catalog.Index(content);
        }

        return catalog;
    }

    public bool Knows(string? table) =>
        string.IsNullOrWhiteSpace(table) == false && _columnsByTable.ContainsKey(Bare(table));

    public bool HasColumn(string? table, string? column)
    {
        if (string.IsNullOrWhiteSpace(table) || string.IsNullOrWhiteSpace(column))
            return false;

        return _columnsByTable.TryGetValue(Bare(table), out var columns) && columns.Contains(column.Trim());
    }

    public IReadOnlyCollection<string> Columns(string? table) =>
        string.IsNullOrWhiteSpace(table) == false && _columnsByTable.TryGetValue(Bare(table), out var columns)
            ? columns
            : Array.Empty<string>();

    private void Index(string ddl)
    {
        foreach (Match match in CreateTable.Matches(ddl))
        {
            var table = Bare(match.Groups["name"].Value);
            var body = ReadBalancedBody(ddl, match.Index + match.Length - 1);

            if (body is null)
                continue;

            if (_columnsByTable.TryGetValue(table, out var columns) == false)
                _columnsByTable[table] = columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var column in ColumnNames(body))
                columns.Add(column);
        }
    }

    private static IEnumerable<string> ColumnNames(string body)
    {
        foreach (var item in SplitTopLevel(body))
        {
            var trimmed = item.Trim();
            if (trimmed.Length == 0)
                continue;

            var first = trimmed.Split([' ', '\t', '\r', '\n', '('], StringSplitOptions.RemoveEmptyEntries)
                               .FirstOrDefault();

            if (string.IsNullOrEmpty(first))
                continue;

            if (ConstraintLeaders.Contains(Unquote(first), StringComparer.OrdinalIgnoreCase))
                continue;

            yield return Unquote(first);
        }
    }

    private static IEnumerable<string> SplitTopLevel(string body)
    {
        var depth = 0;
        var start = 0;
        char? quote = null;

        for (var i = 0; i < body.Length; i++)
        {
            var c = body[i];

            if (quote is not null)
            {
                if (c == quote)
                    quote = null;
                continue;
            }

            switch (c)
            {
                case '\'' or '"' or '`':
                    quote = c;
                    break;
                case '[':
                    quote = ']';
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
        char? quote = null;

        for (var i = openParenIndex; i < ddl.Length; i++)
        {
            var c = ddl[i];

            if (quote is not null)
            {
                if (c == quote)
                    quote = null;
                continue;
            }

            switch (c)
            {
                case '\'' or '"' or '`':
                    quote = c;
                    break;
                case '[':
                    quote = ']';
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

    private static string Bare(string table)
    {
        var name = table.Trim();
        var lastSeparator = name.LastIndexOf('.');

        if (lastSeparator >= 0)
            name = name[(lastSeparator + 1)..];

        return Unquote(name);
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

    private static string Digest(string content) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
}

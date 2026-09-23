using System.Security.Cryptography;
using System.Text;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Schema;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// The table and column facts the validator holds the model to.
///
/// Built either from DDL files on disk or straight from a discovered schema. The discovery path is
/// exact by construction - it indexes the same data it renders into the DDL the model reads - so
/// the two can never disagree. The file path goes through <see cref="DdlColumnExtractor"/>, and a
/// table it could not read is simply unknown, which makes the column check skip rather than reject
/// a column it only failed to parse.
/// </summary>
public sealed class SchemaCatalog
{
    /// <summary>
    /// Columns are kept in the order the source declares them - that is the order the model reads
    /// them in, and it makes the "Known columns" list in a rejection stable.
    /// </summary>
    private sealed record TableEntry(string Schema, string Name)
    {
        public List<string> Columns { get; } = [];

        public HashSet<string> Lookup { get; } = new(StringComparer.OrdinalIgnoreCase);

        public string Qualified => string.IsNullOrEmpty(Schema) ? Name : $"{Schema}.{Name}";

        public void Add(string column)
        {
            if (Lookup.Add(column))
                Columns.Add(column);
        }
    }

    private readonly Dictionary<string, TableEntry> _byQualified = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<TableEntry>> _byBare = new(StringComparer.OrdinalIgnoreCase);

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
                Digest = Digest(content),

                // Held in memory so the catalog stays usable once the file is gone - a fork that
                // materialised its DDL into a temp directory deletes it as soon as this returns.
                Content = content
            });

            foreach (var declared in DdlColumnExtractor.Extract(content))
                catalog.Add(declared.Schema, declared.Name, declared.Columns);
        }

        return catalog;
    }

    /// <summary>
    /// Build from what discovery already found, rendering one DDL file per table for the model to
    /// read. No parser is involved: the indexed columns are the discovered columns.
    /// </summary>
    public static SchemaCatalog FromDiscoveredSchema(CdcSinkSourceSchema schema)
    {
        var files = new List<SchemaFile>();
        var catalog = new SchemaCatalog(files);

        foreach (var table in schema.Tables ?? [])
        {
            var content = DdlRenderer.Render(table);

            files.Add(new SchemaFile
            {
                Name = DdlRenderer.FileNameFor(table),
                Digest = Digest(content),
                Content = content
            });

            catalog.Add(
                table.SourceTableSchema ?? string.Empty,
                table.SourceTableName ?? string.Empty,
                (table.Columns ?? []).Select(c => c.Name).Where(n => string.IsNullOrWhiteSpace(n) == false).ToArray());
        }

        return catalog;
    }

    /// <summary>True when the name resolves to exactly one table. An ambiguous bare name does not.</summary>
    public bool Knows(string? table) => Resolve(table) is not null;

    public bool HasColumn(string? table, string? column) =>
        string.IsNullOrWhiteSpace(column) == false &&
        Resolve(table) is { } entry &&
        entry.Lookup.Contains(column.Trim());

    public IReadOnlyList<string> Columns(string? table) =>
        Resolve(table)?.Columns ?? (IReadOnlyList<string>)Array.Empty<string>();

    /// <summary>A bare table name declared under more than one schema. The caller must qualify it.</summary>
    public bool IsAmbiguous(string? table) =>
        table is not null &&
        Qualified(table) == false &&
        _byBare.TryGetValue(Bare(table), out var entries) &&
        entries.Count > 1;

    public IReadOnlyList<string> Candidates(string? table) =>
        table is not null && _byBare.TryGetValue(Bare(table), out var entries)
            ? entries.Select(e => e.Qualified).ToArray()
            : Array.Empty<string>();

    private TableEntry? Resolve(string? table)
    {
        if (string.IsNullOrWhiteSpace(table))
            return null;

        if (Qualified(table) && _byQualified.TryGetValue(Normalise(table), out var qualified))
            return qualified;

        if (_byBare.TryGetValue(Bare(table), out var entries) && entries.Count == 1)
            return entries[0];

        return null;
    }

    private void Add(string schema, string name, IReadOnlyCollection<string> columns)
    {
        if (string.IsNullOrWhiteSpace(name))
            return;

        var key = string.IsNullOrEmpty(schema) ? name : $"{schema}.{name}";

        if (_byQualified.TryGetValue(key, out var entry) == false)
        {
            entry = new TableEntry(schema, name);
            _byQualified[key] = entry;

            if (_byBare.TryGetValue(name, out var bare) == false)
                _byBare[name] = bare = new List<TableEntry>();

            bare.Add(entry);
        }

        foreach (var column in columns)
            entry.Add(column.Trim());
    }

    private static bool Qualified(string table) => table.Contains('.');

    private static string Normalise(string table) =>
        string.Join('.', table.Split('.', StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()));

    private static string Bare(string table)
    {
        var name = table.Trim();
        var lastSeparator = name.LastIndexOf('.');

        return lastSeparator >= 0 ? name[(lastSeparator + 1)..].Trim() : name;
    }

    private static string Digest(string content) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
}

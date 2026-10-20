using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Server.Documents.CdcSink.Schema.Ddl;

internal abstract class CdcSinkDdlExporter
{
    private readonly string _factoryName;

    protected CdcSinkDdlExporter(string factoryName)
    {
        _factoryName = factoryName;
    }

    protected abstract StringComparison IdentifierComparison { get; }

    public static CdcSinkDdlExporter For(string factoryName)
    {
        return factoryName switch
        {
            CdcSinkSchemaDiscovery.NpgsqlFactory => new PostgresCdcSinkDdlExporter(),
            CdcSinkSchemaDiscovery.MicrosoftDataSqlClientFactory => new SqlServerCdcSinkDdlExporter(),
            CdcSinkSchemaDiscovery.MySqlDataFactory or CdcSinkSchemaDiscovery.MySqlConnectorFactory => new MySqlCdcSinkDdlExporter(factoryName),
            _ => throw new InvalidOperationException(CdcSinkSchemaDiscovery.UnsupportedProviderMessage(factoryName)),
        };
    }

    public async Task<CdcSinkDdlExport> ExportAsync(string connectionString, string[] schemas, string[] tables, CancellationToken ct)
    {
        var discovered = await CdcSinkSchemaDiscovery.For(_factoryName).DiscoverAsync(connectionString, schemas, ct);

        var export = new CdcSinkDdlExport { CatalogName = discovered.CatalogName };

        var selected = SelectTables(discovered.Tables, tables)
            .OrderBy(t => t.SourceTableSchema, StringComparer.Ordinal)
            .ThenBy(t => t.SourceTableName, StringComparer.Ordinal)
            .ToList();

        if (selected.Count == 0)
            return export;

        var script = await ScriptAsync(connectionString, selected, ct);

        var usedFileNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { CdcSinkDdlResult.PartitionsFileName, CdcSinkDdlResult.ForeignKeysFileName };
        foreach (var table in selected)
        {
            if (script.TableScripts.TryGetValue((table.SourceTableSchema, table.SourceTableName), out var sql) == false)
                throw new InvalidOperationException($"Could not read the definition of table '{table.SourceTableSchema}.{table.SourceTableName}' from the source database.");

            export.Files.Add(new CdcSinkDdlFile(UniqueFileName(table, usedFileNames), sql));
        }

        if (script.Partitions.Length > 0)
            export.Files.Add(new CdcSinkDdlFile(CdcSinkDdlResult.PartitionsFileName, script.Partitions.ToString()));

        if (script.ForeignKeys.Length > 0)
            export.Files.Add(new CdcSinkDdlFile(CdcSinkDdlResult.ForeignKeysFileName, script.ForeignKeys.ToString()));

        return export;
    }

    protected abstract Task<CdcSinkDdlScript> ScriptAsync(string connectionString, List<CdcSinkSourceTable> tables, CancellationToken ct);

    private IEnumerable<CdcSinkSourceTable> SelectTables(List<CdcSinkSourceTable> discovered, string[] filter)
    {
        if (filter is not { Length: > 0 })
            return discovered;

        return discovered.Where(t => filter.Any(f =>
            string.Equals(f, t.SourceTableName, IdentifierComparison) ||
            string.Equals(f, $"{t.SourceTableSchema}.{t.SourceTableName}", IdentifierComparison)));
    }

    private static string UniqueFileName(CdcSinkSourceTable table, HashSet<string> usedFileNames)
    {
        var baseName = $"{SanitizeFileNamePart(table.SourceTableSchema)}/{SanitizeFileNamePart(table.SourceTableName)}";
        var fileName = baseName + ".sql";
        for (var i = 2; usedFileNames.Add(fileName) == false; i++)
            fileName = $"{baseName}_{i}.sql";
        return fileName;
    }

    private static string SanitizeFileNamePart(string name)
    {
        if (string.IsNullOrEmpty(name))
            return "_";

        var sb = new StringBuilder(name.Length);
        foreach (var c in name)
            sb.Append(c is '/' or '\\' or ':' or '*' or '?' or '"' or '<' or '>' or '|' || char.IsControl(c) ? '_' : c);

        var result = sb.ToString();
        return result is "." or ".." ? "_" + result : result;
    }

    protected sealed class CdcSinkDdlScript
    {
        public Dictionary<(string Schema, string Table), string> TableScripts { get; } = new();

        public StringBuilder Partitions { get; } = new();

        public StringBuilder ForeignKeys { get; } = new();
    }
}

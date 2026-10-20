using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Server.Documents.CdcSink.Schema.Ddl;

internal sealed class MySqlCdcSinkDdlExporter : CdcSinkDdlExporter
{
    private const string SelectTableTypesQuery =
        "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()";

    private const string SelectForeignKeysQuery = @"
SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME,
       r.UPDATE_RULE, r.DELETE_RULE
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
  JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r
    ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND r.TABLE_NAME = k.TABLE_NAME
 WHERE k.TABLE_SCHEMA = DATABASE() AND k.REFERENCED_TABLE_NAME IS NOT NULL
 ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION";

    private static readonly Regex ForeignKeyLine = new(@"^\s*CONSTRAINT\s+`(?:[^`]|``)+`\s+FOREIGN\s+KEY\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public MySqlCdcSinkDdlExporter(string factoryName) : base(factoryName)
    {
    }

    protected override StringComparison IdentifierComparison => StringComparison.OrdinalIgnoreCase;

    protected override async Task<CdcSinkDdlScript> ScriptAsync(string connectionString, List<CdcSinkSourceTable> tables, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync(ct);

        var tableTypes = new Dictionary<string, string>(StringComparer.Ordinal);
        await using (var cmd = new MySqlCommand(SelectTableTypesQuery, conn))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                tableTypes[reader.GetString(0)] = reader.GetString(1);
        }

        var script = new CdcSinkDdlScript();
        foreach (var table in tables)
        {
            var isSequence = tableTypes.TryGetValue(table.SourceTableName, out var type) && string.Equals(type, "SEQUENCE", StringComparison.OrdinalIgnoreCase);
            var statement = (isSequence ? "SHOW CREATE SEQUENCE " : "SHOW CREATE TABLE ") + QuoteIdentifier(table.SourceTableName);

            await using var cmd = new MySqlCommand(statement, conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct) == false)
                continue;

            var ddl = reader.GetString(1);
            script.TableScripts[(table.SourceTableSchema, table.SourceTableName)] = (isSequence ? ddl : RemoveForeignKeys(ddl)) + ";" + Environment.NewLine;
        }

        var selected = new HashSet<string>(tables.Select(t => t.SourceTableName), StringComparer.Ordinal);
        var foreignKeys = new List<ForeignKey>();
        await using (var cmd = new MySqlCommand(SelectForeignKeysQuery, conn))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var tableName = reader.GetString(0);
                if (selected.Contains(tableName) == false)
                    continue;

                var name = reader.GetString(1);
                var fk = foreignKeys.LastOrDefault();
                if (fk == null || fk.Table != tableName || fk.Name != name)
                {
                    var referencedSchema = reader.GetString(3);
                    var referencedTable = QuoteIdentifier(reader.GetString(4));
                    if (string.Equals(referencedSchema, conn.Database, StringComparison.Ordinal) == false)
                        referencedTable = QuoteIdentifier(referencedSchema) + "." + referencedTable;

                    fk = new ForeignKey(tableName, name, referencedTable, reader.GetString(6), reader.GetString(7));
                    foreignKeys.Add(fk);
                }
                fk.Columns.Add(QuoteIdentifier(reader.GetString(2)));
                fk.ReferencedColumns.Add(QuoteIdentifier(reader.GetString(5)));
            }
        }

        foreach (var fk in foreignKeys)
            script.ForeignKeys.AppendLine(fk.ToSql());

        return script;
    }

    internal static string RemoveForeignKeys(string createTable)
    {
        var lines = createTable.Split('\n').Select(l => l.TrimEnd('\r')).Where(l => ForeignKeyLine.IsMatch(l) == false).ToList();

        for (var i = 0; i < lines.Count - 1; i++)
        {
            if (lines[i + 1].TrimStart().StartsWith(")", StringComparison.Ordinal) && lines[i].EndsWith(",", StringComparison.Ordinal))
                lines[i] = lines[i].Substring(0, lines[i].Length - 1);
        }

        return string.Join("\n", lines);
    }

    private static string QuoteIdentifier(string identifier) => "`" + identifier.Replace("`", "``") + "`";

    private static string ReferentialAction(string rule) => rule is "CASCADE" or "SET NULL" or "SET DEFAULT" ? rule : null;

    private sealed record ForeignKey(string Table, string Name, string ReferencedTable, string UpdateRule, string DeleteRule)
    {
        public List<string> Columns { get; } = new();

        public List<string> ReferencedColumns { get; } = new();

        public string ToSql()
        {
            var sb = new StringBuilder("ALTER TABLE ");
            sb.Append(QuoteIdentifier(Table)).Append(" ADD CONSTRAINT ").Append(QuoteIdentifier(Name))
                .Append(" FOREIGN KEY (").Append(string.Join(", ", Columns)).Append(") REFERENCES ")
                .Append(ReferencedTable).Append(" (").Append(string.Join(", ", ReferencedColumns)).Append(')');

            var onDelete = ReferentialAction(DeleteRule);
            if (onDelete != null)
                sb.Append(" ON DELETE ").Append(onDelete);
            var onUpdate = ReferentialAction(UpdateRule);
            if (onUpdate != null)
                sb.Append(" ON UPDATE ").Append(onUpdate);
            return sb.Append(';').ToString();
        }
    }
}

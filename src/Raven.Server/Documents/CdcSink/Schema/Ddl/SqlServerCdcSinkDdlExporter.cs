using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Server.Documents.CdcSink.Schema.Ddl;

internal sealed class SqlServerCdcSinkDdlExporter : CdcSinkDdlExporter
{
    private const string SelectDatabaseCollationQuery = "SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))";

    private const string SelectTablesQuery = @"
SELECT t.object_id, s.name, t.name
  FROM sys.tables t
  JOIN sys.schemas s ON s.schema_id = t.schema_id
 WHERE t.is_ms_shipped = 0";

    private const string SelectColumnsQuery = @"
SELECT c.object_id, c.name, ty.name AS type_name, ty.is_user_defined, SCHEMA_NAME(ty.schema_id) AS type_schema,
       c.max_length, c.precision, c.scale, c.is_nullable, c.collation_name, c.is_rowguidcol, c.is_sparse,
       ic.seed_value, ic.increment_value,
       cc.definition AS computed_definition, cc.is_persisted,
       dc.name AS default_name, dc.definition AS default_definition
  FROM sys.columns c
  JOIN sys.tables t ON t.object_id = c.object_id
  JOIN sys.types ty ON ty.user_type_id = c.user_type_id
  LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
  LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
  LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
 WHERE t.is_ms_shipped = 0
 ORDER BY c.object_id, c.column_id";

    private const string SelectKeyConstraintsQuery = @"
SELECT kc.parent_object_id, kc.name, kc.type, i.type_desc, col.name AS column_name, ic.is_descending_key
  FROM sys.key_constraints kc
  JOIN sys.tables t ON t.object_id = kc.parent_object_id
  JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
  JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
  JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
 WHERE t.is_ms_shipped = 0
 ORDER BY kc.parent_object_id, CASE kc.type WHEN 'PK' THEN 0 ELSE 1 END, kc.name, ic.key_ordinal";

    private const string SelectCheckConstraintsQuery = @"
SELECT cc.parent_object_id, cc.name, cc.definition
  FROM sys.check_constraints cc
  JOIN sys.tables t ON t.object_id = cc.parent_object_id
 WHERE t.is_ms_shipped = 0
 ORDER BY cc.parent_object_id, cc.name";

    private const string SelectIndexesQuery = @"
SELECT i.object_id, i.name, i.is_unique, i.type_desc, i.filter_definition,
       col.name AS column_name, ic.is_descending_key, ic.is_included_column
  FROM sys.indexes i
  JOIN sys.tables t ON t.object_id = i.object_id
  JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
  JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
 WHERE t.is_ms_shipped = 0
   AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 AND i.is_hypothetical = 0
   AND i.type IN (1, 2)
 ORDER BY i.object_id, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id";

    private const string SelectForeignKeysQuery = @"
SELECT fk.parent_object_id, fk.name, SCHEMA_NAME(rt.schema_id) AS referenced_schema, rt.name AS referenced_table,
       fk.delete_referential_action, fk.update_referential_action, fk.is_not_for_replication,
       pc.name AS column_name, rc.name AS referenced_column_name
  FROM sys.foreign_keys fk
  JOIN sys.tables t ON t.object_id = fk.parent_object_id
  JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
  JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
 WHERE t.is_ms_shipped = 0
 ORDER BY fk.parent_object_id, fk.name, fkc.constraint_column_id";

    public SqlServerCdcSinkDdlExporter() : base(CdcSinkSchemaDiscovery.MicrosoftDataSqlClientFactory)
    {
    }

    protected override StringComparison IdentifierComparison => StringComparison.OrdinalIgnoreCase;

    protected override async Task<CdcSinkDdlScript> ScriptAsync(string connectionString, List<CdcSinkSourceTable> tables, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);

        var databaseCollation = await ReadScalarAsync(conn, SelectDatabaseCollationQuery, ct);

        var wanted = new HashSet<(string, string)>(tables.Select(t => (t.SourceTableSchema, t.SourceTableName)));
        var selected = new Dictionary<int, (string Schema, string Name)>();
        await ReadAsync(conn, SelectTablesQuery, reader =>
        {
            var key = (reader.GetString(1), reader.GetString(2));
            if (wanted.Contains(key))
                selected[reader.GetInt32(0)] = key;
        }, ct);

        var columns = new Dictionary<int, List<string>>();
        await ReadAsync(conn, SelectColumnsQuery, reader =>
        {
            var objectId = reader.GetInt32(0);
            if (selected.ContainsKey(objectId))
                GetList(columns, objectId).Add(BuildColumnDefinition(reader, databaseCollation));
        }, ct);

        var keyConstraints = new Dictionary<int, List<KeyConstraint>>();
        await ReadAsync(conn, SelectKeyConstraintsQuery, reader =>
        {
            var objectId = reader.GetInt32(0);
            if (selected.ContainsKey(objectId) == false)
                return;

            var list = GetList(keyConstraints, objectId);
            var name = reader.GetString(1);
            var constraint = list.LastOrDefault();
            if (constraint == null || constraint.Name != name)
            {
                constraint = new KeyConstraint(name, reader.GetString(2).Trim() == "PK", reader.GetString(3));
                list.Add(constraint);
            }
            constraint.Columns.Add(QuoteIdentifier(reader.GetString(4)) + (reader.GetBoolean(5) ? " DESC" : " ASC"));
        }, ct);

        var checkConstraints = new Dictionary<int, List<string>>();
        await ReadAsync(conn, SelectCheckConstraintsQuery, reader =>
        {
            var objectId = reader.GetInt32(0);
            if (selected.ContainsKey(objectId))
                GetList(checkConstraints, objectId).Add($"CONSTRAINT {QuoteIdentifier(reader.GetString(1))} CHECK {reader.GetString(2)}");
        }, ct);

        var indexes = new Dictionary<int, List<TableIndex>>();
        await ReadAsync(conn, SelectIndexesQuery, reader =>
        {
            var objectId = reader.GetInt32(0);
            if (selected.ContainsKey(objectId) == false)
                return;

            var list = GetList(indexes, objectId);
            var name = reader.GetString(1);
            var index = list.LastOrDefault();
            if (index == null || index.Name != name)
            {
                index = new TableIndex(name, reader.GetBoolean(2), reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4));
                list.Add(index);
            }

            var column = QuoteIdentifier(reader.GetString(5));
            if (reader.GetBoolean(7))
                index.IncludedColumns.Add(column);
            else
                index.KeyColumns.Add(column + (reader.GetBoolean(6) ? " DESC" : " ASC"));
        }, ct);

        var foreignKeys = new Dictionary<int, List<ForeignKey>>();
        await ReadAsync(conn, SelectForeignKeysQuery, reader =>
        {
            var objectId = reader.GetInt32(0);
            if (selected.ContainsKey(objectId) == false)
                return;

            var list = GetList(foreignKeys, objectId);
            var name = reader.GetString(1);
            var fk = list.LastOrDefault();
            if (fk == null || fk.Name != name)
            {
                fk = new ForeignKey(
                    name,
                    QualifiedName(reader.GetString(2), reader.GetString(3)),
                    reader.GetByte(4),
                    reader.GetByte(5),
                    reader.GetBoolean(6));
                list.Add(fk);
            }
            fk.Columns.Add(QuoteIdentifier(reader.GetString(7)));
            fk.ReferencedColumns.Add(QuoteIdentifier(reader.GetString(8)));
        }, ct);

        var script = new CdcSinkDdlScript();
        foreach (var (objectId, table) in selected.OrderBy(x => x.Value.Schema, StringComparer.Ordinal).ThenBy(x => x.Value.Name, StringComparer.Ordinal))
        {
            var qualifiedName = QualifiedName(table.Schema, table.Name);
            var definitions = new List<string>();
            if (columns.TryGetValue(objectId, out var tableColumns))
                definitions.AddRange(tableColumns);
            if (keyConstraints.TryGetValue(objectId, out var tableKeys))
                definitions.AddRange(tableKeys.Select(k => k.ToSql()));
            if (checkConstraints.TryGetValue(objectId, out var tableChecks))
                definitions.AddRange(tableChecks);

            var sb = new StringBuilder();
            sb.Append("CREATE TABLE ").Append(qualifiedName).AppendLine(" (");
            for (var i = 0; i < definitions.Count; i++)
            {
                sb.Append("    ").Append(definitions[i]);
                if (i < definitions.Count - 1)
                    sb.Append(',');
                sb.AppendLine();
            }
            sb.AppendLine(");");

            if (indexes.TryGetValue(objectId, out var tableIndexes))
            {
                foreach (var index in tableIndexes)
                    sb.AppendLine(index.ToSql(qualifiedName));
            }

            script.TableScripts[table] = sb.ToString();

            if (foreignKeys.TryGetValue(objectId, out var tableForeignKeys))
            {
                foreach (var fk in tableForeignKeys)
                    script.ForeignKeys.AppendLine(fk.ToSql(qualifiedName));
            }
        }

        return script;
    }

    private static string BuildColumnDefinition(SqlDataReader reader, string databaseCollation)
    {
        var sb = new StringBuilder();
        sb.Append(QuoteIdentifier(reader.GetString(1)));

        if (reader.IsDBNull(14) == false)
        {
            sb.Append(" AS ").Append(reader.GetString(14));
            if (reader.GetBoolean(15))
            {
                sb.Append(" PERSISTED");
                if (reader.GetBoolean(8) == false)
                    sb.Append(" NOT NULL");
            }
            return sb.ToString();
        }

        var typeName = reader.GetString(2);
        sb.Append(' ').Append(reader.GetBoolean(3)
            ? QualifiedName(reader.GetString(4), typeName)
            : FormatSystemType(typeName, reader.GetInt16(5), reader.GetByte(6), reader.GetByte(7)));

        if (reader.GetBoolean(11))
            sb.Append(" SPARSE");

        if (reader.IsDBNull(9) == false && reader.GetBoolean(3) == false && string.Equals(reader.GetString(9), databaseCollation, StringComparison.OrdinalIgnoreCase) == false)
            sb.Append(" COLLATE ").Append(reader.GetString(9));

        if (reader.IsDBNull(12) == false)
            sb.Append(" IDENTITY(")
                .Append(Convert.ToString(reader.GetValue(12), CultureInfo.InvariantCulture)).Append(", ")
                .Append(Convert.ToString(reader.GetValue(13), CultureInfo.InvariantCulture)).Append(')');

        if (reader.GetBoolean(10))
            sb.Append(" ROWGUIDCOL");

        sb.Append(reader.GetBoolean(8) ? " NULL" : " NOT NULL");

        if (reader.IsDBNull(17) == false)
            sb.Append(" CONSTRAINT ").Append(QuoteIdentifier(reader.GetString(16))).Append(" DEFAULT ").Append(reader.GetString(17));

        return sb.ToString();
    }

    private static string FormatSystemType(string typeName, short maxLength, byte precision, byte scale)
    {
        switch (typeName)
        {
            case "varchar":
            case "char":
            case "varbinary":
            case "binary":
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength})";
            case "nvarchar":
            case "nchar":
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength / 2})";
            case "decimal":
            case "numeric":
                return $"{typeName}({precision}, {scale})";
            case "datetime2":
            case "time":
            case "datetimeoffset":
                return $"{typeName}({scale})";
            case "float":
                return precision == 53 ? typeName : $"{typeName}({precision})";
            default:
                return typeName;
        }
    }

    private static string QuoteIdentifier(string identifier) => "[" + identifier.Replace("]", "]]") + "]";

    private static string QualifiedName(string schema, string name) => QuoteIdentifier(schema) + "." + QuoteIdentifier(name);

    private static List<T> GetList<T>(Dictionary<int, List<T>> dictionary, int key)
    {
        if (dictionary.TryGetValue(key, out var list) == false)
            dictionary[key] = list = new List<T>();
        return list;
    }

    private static async Task<string> ReadScalarAsync(SqlConnection conn, string query, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(query, conn);
        return (await cmd.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task ReadAsync(SqlConnection conn, string query, Action<SqlDataReader> onRow, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(query, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            onRow(reader);
    }

    private static string ReferentialAction(byte action) => action switch
    {
        1 => "CASCADE",
        2 => "SET NULL",
        3 => "SET DEFAULT",
        _ => null,
    };

    private sealed record KeyConstraint(string Name, bool IsPrimaryKey, string IndexType)
    {
        public List<string> Columns { get; } = new();

        public string ToSql() =>
            $"CONSTRAINT {QuoteIdentifier(Name)} {(IsPrimaryKey ? "PRIMARY KEY" : "UNIQUE")} {IndexType} ({string.Join(", ", Columns)})";
    }

    private sealed record TableIndex(string Name, bool IsUnique, string IndexType, string Filter)
    {
        public List<string> KeyColumns { get; } = new();

        public List<string> IncludedColumns { get; } = new();

        public string ToSql(string qualifiedTableName)
        {
            var sb = new StringBuilder("CREATE ");
            if (IsUnique)
                sb.Append("UNIQUE ");
            sb.Append(IndexType).Append(" INDEX ").Append(QuoteIdentifier(Name))
                .Append(" ON ").Append(qualifiedTableName)
                .Append(" (").Append(string.Join(", ", KeyColumns)).Append(')');
            if (IncludedColumns.Count > 0)
                sb.Append(" INCLUDE (").Append(string.Join(", ", IncludedColumns)).Append(')');
            if (Filter != null)
                sb.Append(" WHERE ").Append(Filter);
            return sb.Append(';').ToString();
        }
    }

    private sealed record ForeignKey(string Name, string ReferencedTable, byte DeleteAction, byte UpdateAction, bool NotForReplication)
    {
        public List<string> Columns { get; } = new();

        public List<string> ReferencedColumns { get; } = new();

        public string ToSql(string qualifiedTableName)
        {
            var sb = new StringBuilder("ALTER TABLE ");
            sb.Append(qualifiedTableName).Append(" ADD CONSTRAINT ").Append(QuoteIdentifier(Name))
                .Append(" FOREIGN KEY (").Append(string.Join(", ", Columns)).Append(") REFERENCES ")
                .Append(ReferencedTable).Append(" (").Append(string.Join(", ", ReferencedColumns)).Append(')');

            var onDelete = ReferentialAction(DeleteAction);
            if (onDelete != null)
                sb.Append(" ON DELETE ").Append(onDelete);
            var onUpdate = ReferentialAction(UpdateAction);
            if (onUpdate != null)
                sb.Append(" ON UPDATE ").Append(onUpdate);
            if (NotForReplication)
                sb.Append(" NOT FOR REPLICATION");
            return sb.Append(';').ToString();
        }
    }
}

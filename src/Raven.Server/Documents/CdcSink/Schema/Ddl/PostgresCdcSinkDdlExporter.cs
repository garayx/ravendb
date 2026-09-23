using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Server.Documents.CdcSink.Schema.Ddl;

internal sealed class PostgresCdcSinkDdlExporter : CdcSinkDdlExporter
{
    private const string SelectRelationsQuery = @"
SELECT c.oid, n.nspname, c.relname, c.relkind, c.relpersistence, c.relispartition,
       CASE WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid) END AS partkey,
       CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid) END AS partbound,
       (SELECT ARRAY[pn.nspname::text, pc.relname::text]
          FROM pg_inherits i
          JOIN pg_class pc ON pc.oid = i.inhparent
          JOIN pg_namespace pn ON pn.oid = pc.relnamespace
         WHERE i.inhrelid = c.oid AND c.relispartition
         LIMIT 1) AS partparent,
       (SELECT s.srvname::text FROM pg_foreign_table ft JOIN pg_foreign_server s ON s.oid = ft.ftserver WHERE ft.ftrelid = c.oid) AS ftserver,
       (SELECT ft.ftoptions FROM pg_foreign_table ft WHERE ft.ftrelid = c.oid) AS ftoptions
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN unnest(@schemas::text[], @tables::text[]) AS x(s, t) ON x.s = n.nspname AND x.t = c.relname
 WHERE c.relkind IN ('r', 'p', 'f')";

    private const string SelectColumnsQueryTemplate = @"
SELECT a.attrelid, a.attname, format_type(a.atttypid, a.atttypmod) AS typename, a.atttypid::int8 AS typeoid,
       a.attnotnull, a.attidentity::text AS attidentity, {0} AS attgenerated,
       pg_get_expr(d.adbin, d.adrelid) AS defexpr,
       CASE WHEN a.attcollation <> 0 AND a.attcollation <> t.typcollation
            THEN quote_ident(cn.nspname) || '.' || quote_ident(co.collname) END AS collation,
       CASE WHEN a.attidentity = '' AND d.adbin IS NOT NULL
            THEN pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(c.relname), a.attname) END AS serialseq
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_type t ON t.oid = a.atttypid
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  LEFT JOIN pg_collation co ON co.oid = a.attcollation
  LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
 WHERE a.attrelid = ANY(@oids) AND a.attnum > 0 AND NOT a.attisdropped
 ORDER BY a.attrelid, a.attnum";

    private const string SelectConstraintsQueryTemplate = @"
SELECT c.conrelid, c.conname, c.contype::text AS contype, pg_get_constraintdef(c.oid, true) AS condef,
       c.convalidated, c.coninhcount > 0 AS hasparent, {0} AS inherited
  FROM pg_constraint c
 WHERE c.conrelid = ANY(@oids) AND c.contype IN ('p', 'u', 'c', 'x', 'f')
 ORDER BY c.conrelid,
          CASE c.contype WHEN 'p' THEN 0 WHEN 'u' THEN 1 WHEN 'x' THEN 2 WHEN 'c' THEN 3 ELSE 4 END,
          c.conname";

    private const string SelectIndexesQuery = @"
SELECT i.indrelid, pg_get_indexdef(i.indexrelid) AS indexdef
  FROM pg_index i
  JOIN pg_class ic ON ic.oid = i.indexrelid
 WHERE i.indrelid = ANY(@oids)
   AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid AND c.conrelid = i.indrelid AND c.contype IN ('p', 'u', 'x'))
   AND NOT EXISTS (SELECT 1 FROM pg_inherits h WHERE h.inhrelid = i.indexrelid)
 ORDER BY i.indrelid, ic.relname";

    private static readonly Dictionary<long, string> SerialTypes = new()
    {
        [21] = "smallserial",
        [23] = "serial",
        [20] = "bigserial",
    };

    public PostgresCdcSinkDdlExporter() : base(CdcSinkSchemaDiscovery.NpgsqlFactory)
    {
    }

    protected override StringComparison IdentifierComparison => StringComparison.Ordinal;

    protected override async Task<CdcSinkDdlScript> ScriptAsync(string connectionString, List<CdcSinkSourceTable> tables, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);

        await using var tx = await conn.BeginTransactionAsync(ct);

        // Forces pg_get_* and format_type to schema-qualify every user object.
        await using (var cmd = new NpgsqlCommand("SET LOCAL search_path = ''", conn, tx))
            await cmd.ExecuteNonQueryAsync(ct);

        var relations = await ReadRelationsAsync(conn, tx, tables, ct);
        var oids = relations.Keys.ToArray();

        var version = conn.PostgreSqlVersion;
        var columns = await ReadColumnsAsync(conn, tx, oids, version.Major >= 12, ct);
        var constraints = await ReadConstraintsAsync(conn, tx, oids, version.Major >= 11, ct);
        var indexes = await ReadIndexesAsync(conn, tx, oids, ct);

        var script = new CdcSinkDdlScript();
        foreach (var (oid, relation) in relations.OrderBy(r => r.Value.Schema, StringComparer.Ordinal).ThenBy(r => r.Value.Name, StringComparer.Ordinal))
        {
            var tableConstraints = constraints.TryGetValue(oid, out var c) ? c : new List<PgConstraint>();
            script.TableScripts[(relation.Schema, relation.Name)] = BuildTableScript(
                relation,
                columns.TryGetValue(oid, out var cols) ? cols : new List<PgColumn>(),
                tableConstraints,
                indexes.TryGetValue(oid, out var idx) ? idx : new List<string>());

            foreach (var fk in tableConstraints.Where(x => x.Type == "f" && x.Inherited == false))
                script.ForeignKeys.Append("ALTER TABLE ").Append(relation.QualifiedName)
                    .Append(" ADD CONSTRAINT ").Append(QuoteIdentifier(fk.Name)).Append(' ').Append(fk.Definition).AppendLine(";");
        }

        return script;
    }

    private static string BuildTableScript(PgRelation relation, List<PgColumn> columns, List<PgConstraint> constraints, List<string> indexes)
    {
        var sb = new StringBuilder();
        var definitions = new List<string>();
        var deferredConstraints = new List<PgConstraint>();

        if (relation.IsPartition == false)
        {
            foreach (var column in columns)
                definitions.Add(BuildColumnDefinition(column));
        }

        foreach (var constraint in constraints)
        {
            if (constraint.Type == "f" || constraint.Inherited || (relation.IsPartition && constraint.HasParent))
                continue;

            if (constraint.Validated == false)
            {
                deferredConstraints.Add(constraint);
                continue;
            }

            definitions.Add($"CONSTRAINT {QuoteIdentifier(constraint.Name)} {constraint.Definition}");
        }

        sb.Append("CREATE ");
        if (relation.Kind == 'f')
            sb.Append("FOREIGN ");
        else if (relation.Persistence == 'u')
            sb.Append("UNLOGGED ");
        sb.Append("TABLE ").Append(relation.QualifiedName);

        if (relation.IsPartition)
        {
            sb.Append(" PARTITION OF ").Append(relation.PartitionParent);
            if (definitions.Count > 0)
                AppendDefinitions(sb, definitions);
            sb.Append(' ').Append(relation.PartitionBound);
        }
        else
        {
            AppendDefinitions(sb, definitions);
        }

        if (relation.PartitionKey != null)
            sb.Append(" PARTITION BY ").Append(relation.PartitionKey);

        if (relation.Kind == 'f')
        {
            sb.Append(" SERVER ").Append(relation.ForeignServer);
            if (relation.ForeignOptions is { Length: > 0 })
                sb.Append(" OPTIONS (").Append(string.Join(", ", relation.ForeignOptions.Select(FormatOption))).Append(')');
        }

        sb.AppendLine(";");

        foreach (var constraint in deferredConstraints)
            sb.Append("ALTER TABLE ").Append(relation.QualifiedName).Append(" ADD CONSTRAINT ")
                .Append(QuoteIdentifier(constraint.Name)).Append(' ').Append(constraint.Definition).AppendLine(";");

        foreach (var index in indexes)
            sb.Append(relation.Kind == 'p' ? index.Replace(" ON ONLY ", " ON ") : index).AppendLine(";");

        return sb.ToString();
    }

    private static void AppendDefinitions(StringBuilder sb, List<string> definitions)
    {
        sb.AppendLine(" (");
        for (var i = 0; i < definitions.Count; i++)
        {
            sb.Append("    ").Append(definitions[i]);
            if (i < definitions.Count - 1)
                sb.Append(',');
            sb.AppendLine();
        }
        sb.Append(')');
    }

    private static string BuildColumnDefinition(PgColumn column)
    {
        var sb = new StringBuilder();
        sb.Append(QuoteIdentifier(column.Name));

        if (column.SerialSequence != null && SerialTypes.TryGetValue(column.TypeOid, out var serialType)
            && column.Default != null && column.Default.StartsWith("nextval(", StringComparison.Ordinal))
            return sb.Append(' ').Append(serialType).ToString();

        sb.Append(' ').Append(column.TypeName);

        if (column.Collation != null)
            sb.Append(" COLLATE ").Append(column.Collation);

        if (column.Generated == "s")
            sb.Append(" GENERATED ALWAYS AS (").Append(column.Default).Append(") STORED");
        else if (column.Identity == "a")
            sb.Append(" GENERATED ALWAYS AS IDENTITY");
        else if (column.Identity == "d")
            sb.Append(" GENERATED BY DEFAULT AS IDENTITY");
        else if (column.Default != null)
            sb.Append(" DEFAULT ").Append(column.Default);

        if (column.NotNull)
            sb.Append(" NOT NULL");

        return sb.ToString();
    }

    private static string FormatOption(string option)
    {
        var separator = option.IndexOf('=');
        if (separator < 0)
            return option;
        return $"{option.Substring(0, separator)} {QuoteLiteral(option.Substring(separator + 1))}";
    }

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static string QualifiedName(string[] parts) => QuoteIdentifier(parts[0]) + "." + QuoteIdentifier(parts[1]);

    private static string QuoteLiteral(string value) => "'" + value.Replace("'", "''") + "'";

    private static async Task<Dictionary<uint, PgRelation>> ReadRelationsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<CdcSinkSourceTable> tables, CancellationToken ct)
    {
        var result = new Dictionary<uint, PgRelation>();

        await using var cmd = new NpgsqlCommand(SelectRelationsQuery, conn, tx);
        cmd.Parameters.AddWithValue("schemas", tables.Select(t => t.SourceTableSchema).ToArray());
        cmd.Parameters.AddWithValue("tables", tables.Select(t => t.SourceTableName).ToArray());

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var schema = reader.GetString(1);
            var name = reader.GetString(2);
            result[reader.GetFieldValue<uint>(0)] = new PgRelation
            {
                Schema = schema,
                Name = name,
                QualifiedName = QualifiedName([schema, name]),
                Kind = reader.GetChar(3),
                Persistence = reader.GetChar(4),
                IsPartition = reader.GetBoolean(5),
                PartitionKey = reader.IsDBNull(6) ? null : reader.GetString(6),
                PartitionBound = reader.IsDBNull(7) ? null : reader.GetString(7),
                PartitionParent = reader.IsDBNull(8) ? null : QualifiedName(reader.GetFieldValue<string[]>(8)),
                ForeignServer = reader.IsDBNull(9) ? null : QuoteIdentifier(reader.GetString(9)),
                ForeignOptions = reader.IsDBNull(10) ? null : reader.GetFieldValue<string[]>(10),
            };
        }

        return result;
    }

    private static async Task<Dictionary<uint, List<PgColumn>>> ReadColumnsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, uint[] oids, bool supportsGeneratedColumns, CancellationToken ct)
    {
        var result = new Dictionary<uint, List<PgColumn>>();

        var query = string.Format(SelectColumnsQueryTemplate, supportsGeneratedColumns ? "a.attgenerated::text" : "''::text");
        await using var cmd = new NpgsqlCommand(query, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter("oids", NpgsqlDbType.Array | NpgsqlDbType.Oid) { Value = oids });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var oid = reader.GetFieldValue<uint>(0);
            if (result.TryGetValue(oid, out var list) == false)
                result[oid] = list = new List<PgColumn>();

            list.Add(new PgColumn
            {
                Name = reader.GetString(1),
                TypeName = reader.GetString(2),
                TypeOid = reader.GetInt64(3),
                NotNull = reader.GetBoolean(4),
                Identity = reader.GetString(5),
                Generated = reader.GetString(6),
                Default = reader.IsDBNull(7) ? null : reader.GetString(7),
                Collation = reader.IsDBNull(8) ? null : reader.GetString(8),
                SerialSequence = reader.IsDBNull(9) ? null : reader.GetString(9),
            });
        }

        return result;
    }

    private static async Task<Dictionary<uint, List<PgConstraint>>> ReadConstraintsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, uint[] oids, bool supportsConstraintParent, CancellationToken ct)
    {
        var result = new Dictionary<uint, List<PgConstraint>>();

        var query = string.Format(SelectConstraintsQueryTemplate, supportsConstraintParent ? "c.conparentid <> 0" : "false");
        await using var cmd = new NpgsqlCommand(query, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter("oids", NpgsqlDbType.Array | NpgsqlDbType.Oid) { Value = oids });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var oid = reader.GetFieldValue<uint>(0);
            if (result.TryGetValue(oid, out var list) == false)
                result[oid] = list = new List<PgConstraint>();

            list.Add(new PgConstraint
            {
                Name = reader.GetString(1),
                Type = reader.GetString(2),
                Definition = reader.GetString(3),
                Validated = reader.GetBoolean(4),
                HasParent = reader.GetBoolean(5),
                Inherited = reader.GetBoolean(6),
            });
        }

        return result;
    }

    private static async Task<Dictionary<uint, List<string>>> ReadIndexesAsync(NpgsqlConnection conn, NpgsqlTransaction tx, uint[] oids, CancellationToken ct)
    {
        var result = new Dictionary<uint, List<string>>();

        await using var cmd = new NpgsqlCommand(SelectIndexesQuery, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter("oids", NpgsqlDbType.Array | NpgsqlDbType.Oid) { Value = oids });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var oid = reader.GetFieldValue<uint>(0);
            if (result.TryGetValue(oid, out var list) == false)
                result[oid] = list = new List<string>();
            list.Add(reader.GetString(1));
        }

        return result;
    }

    private sealed class PgRelation
    {
        public string Schema { get; init; }
        public string Name { get; init; }
        public string QualifiedName { get; init; }
        public char Kind { get; init; }
        public char Persistence { get; init; }
        public bool IsPartition { get; init; }
        public string PartitionKey { get; init; }
        public string PartitionBound { get; init; }
        public string PartitionParent { get; init; }
        public string ForeignServer { get; init; }
        public string[] ForeignOptions { get; init; }
    }

    private sealed class PgColumn
    {
        public string Name { get; init; }
        public string TypeName { get; init; }
        public long TypeOid { get; init; }
        public bool NotNull { get; init; }
        public string Identity { get; init; }
        public string Generated { get; init; }
        public string Default { get; init; }
        public string Collation { get; init; }
        public string SerialSequence { get; init; }
    }

    private sealed class PgConstraint
    {
        public string Name { get; init; }
        public string Type { get; init; }
        public string Definition { get; init; }
        public bool Validated { get; init; }
        public bool HasParent { get; init; }
        public bool Inherited { get; init; }
    }
}

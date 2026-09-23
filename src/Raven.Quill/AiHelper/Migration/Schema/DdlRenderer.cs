using System.Text;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Quill.AiHelper.Migration.Schema;

/// <summary>
/// Turns a discovered table into the CREATE TABLE text the agent reads. Discovery already knows
/// the columns, keys and foreign keys exactly, so this renders rather than re-queries - and what
/// it renders is what the catalog indexes, which keeps the two in step by construction.
/// </summary>
public static class DdlRenderer
{
    public static string FileNameFor(CdcSinkSourceTable table) =>
        string.IsNullOrWhiteSpace(table.SourceTableSchema)
            ? $"{table.SourceTableName}.sql"
            : $"{table.SourceTableSchema}.{table.SourceTableName}.sql";

    public static string Render(CdcSinkSourceTable table)
    {
        var sb = new StringBuilder();
        var qualified = string.IsNullOrWhiteSpace(table.SourceTableSchema)
            ? table.SourceTableName
            : $"{table.SourceTableSchema}.{table.SourceTableName}";

        if (string.IsNullOrWhiteSpace(table.UnsupportedReason) == false)
            sb.Append("-- not usable for CDC: ").AppendLine(table.UnsupportedReason);

        foreach (var warning in table.Warnings ?? [])
            sb.Append("-- warning: ").AppendLine(warning);

        if (table.IsCdcEnabled == false)
            sb.AppendLine("-- change data capture is not enabled on this table yet");

        sb.Append("CREATE TABLE ").Append(qualified).AppendLine(" (");

        var lines = new List<string>();

        foreach (var column in table.Columns ?? [])
            lines.Add(RenderColumn(column, table));

        if (table.PrimaryKeyColumns is { Count: > 0 })
            lines.Add($"    PRIMARY KEY ({string.Join(", ", table.PrimaryKeyColumns)})");

        foreach (var fk in table.ForeignKeys ?? [])
        {
            var referenced = string.IsNullOrWhiteSpace(fk.ReferencedSchema)
                ? fk.ReferencedTable
                : $"{fk.ReferencedSchema}.{fk.ReferencedTable}";

            lines.Add($"    FOREIGN KEY ({string.Join(", ", fk.Columns)}) " +
                      $"REFERENCES {referenced} ({string.Join(", ", fk.ReferencedColumns)})");
        }

        sb.AppendLine(string.Join($",{Environment.NewLine}", lines));
        sb.AppendLine(");");

        return sb.ToString();
    }

    private static string RenderColumn(CdcSinkSourceColumn column, CdcSinkSourceTable table)
    {
        var sb = new StringBuilder("    ")
            .Append(column.Name)
            .Append(' ')
            .Append(string.IsNullOrWhiteSpace(column.NativeType) ? "unknown" : column.NativeType);

        var notes = new List<string>();

        if (column.SuggestedType != CdcColumnType.Default)
            notes.Add($"maps best as {column.SuggestedType}");

        if (column.IsCdcCapturable == false)
        {
            notes.Add(string.IsNullOrWhiteSpace(column.UnsupportedReason)
                ? "not captured by CDC"
                : $"not captured by CDC: {column.UnsupportedReason}");
        }

        if (notes.Count > 0)
            sb.Append("  -- ").Append(string.Join("; ", notes));

        return sb.ToString();
    }
}

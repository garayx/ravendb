using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.Contracts;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// Assembles what the session registered into the configuration the rest of the wizard already
/// knows how to carry, and reports which of the discovered tables it does not cover.
/// </summary>
public static class PlanToCdcConfiguration
{
    public static CdcSinkConfiguration Build(
        MigrationPlanSnapshot snapshot,
        string name,
        string connectionStringName) =>
        new()
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            Tables = snapshot.Collections
                .Select(c => c.Config)
                .Where(c => c is not null)
                .Select(c => c!)
                .ToList()
        };

    /// <summary>
    /// The tables the operator discovered that no mapping captures. Coverage counts roots and
    /// embedded tables at any depth; a link does not count, because it only references documents
    /// another mapping has to produce.
    /// </summary>
    public static string[] UnmappedTables(CdcSinkConfiguration configuration, CdcSinkSourceSchema discovered)
    {
        var defaultSchema = DefaultSchemaOf(discovered);

        var covered = configuration
            .CollectAllTablesFlat(defaultSchema)
            .Select(t => t.FullName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return (discovered.Tables ?? [])
            .Select(t => $"{(string.IsNullOrEmpty(t.SourceTableSchema) ? defaultSchema : t.SourceTableSchema)}.{t.SourceTableName}")
            .Where(name => covered.Contains(name) == false)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    /// <summary>
    /// Discovery stamps every table with the schema it came from, so the schema the operator
    /// actually selected is a better default than a per-provider guess.
    /// </summary>
    private static string DefaultSchemaOf(CdcSinkSourceSchema discovered) =>
        (discovered.Tables ?? [])
            .Select(t => t.SourceTableSchema)
            .Where(s => string.IsNullOrWhiteSpace(s) == false)
            .GroupBy(s => s, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .Select(g => g.Key)
            .FirstOrDefault() ?? string.Empty;
}

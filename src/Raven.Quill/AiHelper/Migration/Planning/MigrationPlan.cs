using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// What the model has actually registered so far, as opposed to what it said. Everything here is
/// the product of a validated tool call; prose never reaches it.
/// </summary>
public sealed class MigrationPlan
{
    private readonly Dictionary<string, PlanEntry> _entries = new(StringComparer.OrdinalIgnoreCase);

    public NamingConventions Conventions { get; private set; } = new();

    /// <summary>Rehydrate a plan persisted by an earlier request.</summary>
    public void Restore(MigrationPlanState state)
    {
        _entries.Clear();

        foreach (var entry in state.Entries)
            _entries[entry.Collection] = entry;

        Conventions = state.Conventions ?? new NamingConventions();
    }

    public void SetConventions(NamingConventions conventions) => Conventions = conventions;

    public PlanEntry Upsert(string collection, string? rationale, CdcSinkTableConfig? config)
    {
        var version = _entries.TryGetValue(collection, out var existing) ? existing.Version + 1 : 1;

        var entry = new PlanEntry
        {
            Collection = collection,
            Rationale = rationale,
            Config = config,
            Version = version
        };

        _entries[collection] = entry;
        return entry;
    }

    public bool Remove(string collection) => _entries.Remove(collection);

    public bool TryGet(string collection, out PlanEntry entry) => _entries.TryGetValue(collection, out entry!);

    public string[] CollectionNames() => _entries.Values.Select(e => e.Collection).ToArray();

    public IReadOnlyCollection<PlanEntry> Entries => _entries.Values;

    /// <summary>
    /// Every source table the plan touches, and how. Used to spot the same rows being embedded in
    /// more than one document.
    ///
    /// Keyed by schema-qualified name: two tables called "orders" under different schemas are
    /// different tables, and conflating them would report an overlap that does not exist.
    /// </summary>
    public Dictionary<string, List<TableUse>> TableUsage()
    {
        var usage = new Dictionary<string, List<TableUse>>(StringComparer.OrdinalIgnoreCase);

        void Add(string? table, string collection, TableUseKind kind)
        {
            if (string.IsNullOrWhiteSpace(table))
                return;

            if (usage.TryGetValue(table, out var uses) == false)
                usage[table] = uses = new List<TableUse>();

            uses.Add(new TableUse(collection, kind));
        }

        foreach (var entry in _entries.Values)
        {
            if (entry.Config is null)
                continue;

            // An embedded or linked entry that names no schema of its own belongs to its root's.
            var rootSchema = entry.Config.SourceTableSchema;

            Add(Qualify(rootSchema, entry.Config.SourceTableName), entry.Collection, TableUseKind.Root);

            foreach (var linked in entry.Config.LinkedTables ?? [])
                Add(Qualify(linked.SourceTableSchema ?? rootSchema, linked.SourceTableName), entry.Collection, TableUseKind.Linked);

            CdcSinkConfiguration.ForEachEmbeddedTable(entry.Config.EmbeddedTables, embedded =>
            {
                Add(Qualify(embedded.SourceTableSchema ?? rootSchema, embedded.SourceTableName), entry.Collection, TableUseKind.Embedded);

                foreach (var linked in embedded.LinkedTables ?? [])
                    Add(Qualify(linked.SourceTableSchema ?? rootSchema, linked.SourceTableName), entry.Collection, TableUseKind.Linked);
            });
        }

        return usage;
    }

    /// <summary>
    /// The key a table is known by: "schema.table" where a schema is set, the bare name otherwise.
    /// Shared with the validator so plan usage and schema lookups agree on what counts as one table.
    /// </summary>
    public static string? Qualify(string? tableSchema, string? table) =>
        string.IsNullOrWhiteSpace(tableSchema) || string.IsNullOrWhiteSpace(table)
            ? table
            : $"{tableSchema.Trim()}.{table.Trim()}";
}

using System.Text.Json;
using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// What the model has actually registered so far, as opposed to what it said. Everything here is
/// the product of a validated tool call; prose never reaches it.
/// </summary>
public sealed class MigrationPlan
{
    private readonly Dictionary<string, PlanEntry> _entries = new(StringComparer.OrdinalIgnoreCase);

    public NamingConventions Conventions { get; private set; } = NamingConventions.None;

    public JsonElement? Proposal { get; private set; }

    public void SetProposal(JsonElement proposal) => Proposal = proposal;

    /// <summary>Rehydrate a plan persisted by an earlier request.</summary>
    public void Restore(MigrationPlanState state)
    {
        _entries.Clear();

        foreach (var entry in state.Entries)
            _entries[entry.Collection] = entry;

        Conventions = state.Conventions ?? NamingConventions.None;
        Proposal = ParseProposal(state.ProposalJson);
    }

    /// <summary>
    /// A JsonElement is only valid while the JsonDocument backing it is alive, so the parsed value
    /// is cloned free of it before the document goes away.
    /// </summary>
    public static JsonElement? ParseProposal(string? proposalJson)
    {
        if (string.IsNullOrWhiteSpace(proposalJson))
            return null;

        using var document = JsonDocument.Parse(proposalJson);
        return document.RootElement.Clone();
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

            Add(entry.Config.SourceTableName, entry.Collection, TableUseKind.Root);

            foreach (var linked in entry.Config.LinkedTables ?? [])
                Add(linked.SourceTableName, entry.Collection, TableUseKind.Linked);

            CdcSinkConfiguration.ForEachEmbeddedTable(entry.Config.EmbeddedTables, embedded =>
            {
                Add(embedded.SourceTableName, entry.Collection, TableUseKind.Embedded);

                foreach (var linked in embedded.LinkedTables ?? [])
                    Add(linked.SourceTableName, entry.Collection, TableUseKind.Linked);
            });
        }

        return usage;
    }
}

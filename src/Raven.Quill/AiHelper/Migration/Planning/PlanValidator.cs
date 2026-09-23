using System.Text.RegularExpressions;
using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

public sealed record ValidationResult(List<string> Errors, List<string> Warnings)
{
    public bool Ok => Errors.Count == 0;
    public static ValidationResult Empty() => new(new List<string>(), new List<string>());
}

/// <summary>
/// Deterministic checks on a configuration the model produced, run inside the action handler
/// before the mapping is accepted.
///
/// This is the part that matters most. An LLM will happily emit an embedded table with no
/// JoinColumns, or a PK column that never appears in Columns, and the failure surfaces hours
/// later as a broken initial load. Rejecting the call with a concrete list of errors puts the
/// model in a fix-and-retry loop it is good at, and keeps prose ("I've configured the orders
/// table") from standing in for a configuration that would not actually run.
/// </summary>
public static class PlanValidator
{
    public static ValidationResult Validate(
        string collection,
        CdcSinkTableConfig? config,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        var r = ValidationResult.Empty();

        if (config is null)
        {
            r.Errors.Add("Config is missing.");
            return r;
        }

        if (string.IsNullOrWhiteSpace(config.CollectionName))
            r.Errors.Add("Config.CollectionName is required.");
        else if (string.Equals(config.CollectionName, collection, StringComparison.Ordinal) == false)
            r.Errors.Add($"Collection '{collection}' does not match Config.CollectionName '{config.CollectionName}'.");

        if (string.IsNullOrWhiteSpace(config.SourceTableName))
            r.Errors.Add("Config.SourceTableName is required.");

        ValidateColumns(r, config.SourceTableName, config.PrimaryKeyColumns, config.Columns, schema, plan.Conventions, "root");

        foreach (var linked in config.LinkedTables ?? new List<CdcSinkLinkedTableConfig>())
            ValidateLinked(r, linked, config, plan, schema);

        ValidateEmbedded(r, config.EmbeddedTables, config.SourceTableName, config.PrimaryKeyColumns,
                         collection, plan, schema, depth: 1);

        ValidateDerivedValuePlacement(r, config);
        ValidateTableOwnership(r, collection, config, plan);

        return r;
    }

    private static void ValidateColumns(
        ValidationResult r,
        string? table,
        List<string>? primaryKeys,
        List<CdcColumnMapping>? columns,
        SchemaCatalog schema,
        NamingConventions conventions,
        string what)
    {
        columns ??= new List<CdcColumnMapping>();

        if (columns.Count == 0)
            r.Errors.Add($"{what} '{table}': Columns is empty - a mapping with no columns produces empty documents.");

        if (primaryKeys is null || primaryKeys.Count == 0)
        {
            r.Errors.Add($"{what} '{table}': PrimaryKeyColumns is required - document IDs are derived from it.");
        }
        else
        {
            // Documented requirement: PK columns must also be mapped in Columns.
            foreach (var pk in primaryKeys)
            {
                if (columns.Any(c => string.Equals(c.Column, pk, StringComparison.OrdinalIgnoreCase)) == false)
                    r.Errors.Add($"{what} '{table}': primary key column '{pk}' is not present in Columns.");
                else if (columns.Any(c => string.Equals(c.Column, pk, StringComparison.OrdinalIgnoreCase) &&
                                          c.Type == CdcColumnType.Attachment))
                    r.Errors.Add($"{what} '{table}': primary key column '{pk}' is mapped as an Attachment.");
            }
        }

        foreach (var group in columns.Where(c => string.IsNullOrWhiteSpace(c.Name) == false)
                                     .GroupBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
                                     .Where(g => g.Count() > 1))
        {
            r.Errors.Add($"{what} '{table}': property name '{group.Key}' is mapped {group.Count()} times " +
                         $"(from {string.Join(", ", group.Select(c => c.Column))}).");
        }

        foreach (var col in columns)
        {
            if (string.IsNullOrWhiteSpace(col.Column))
                r.Errors.Add($"{what} '{table}': a column mapping has no Column.");
            if (string.IsNullOrWhiteSpace(col.Name))
                r.Errors.Add($"{what} '{table}': column '{col.Column}' has no target Name.");

            // The model does not get to invent columns. If we parsed the DDL, hold it to it.
            if (schema.Knows(table) && string.IsNullOrWhiteSpace(col.Column) == false &&
                schema.HasColumn(table, col.Column) == false)
            {
                r.Errors.Add($"{what} '{table}': column '{col.Column}' does not exist in the source schema. " +
                             $"Known columns: {string.Join(", ", schema.Columns(table))}.");
            }

            if (string.IsNullOrWhiteSpace(col.Name) == false && MatchesCase(col.Name, conventions.PropertyCase) == false)
            {
                r.Errors.Add($"{what} '{table}': property '{col.Name}' does not follow the agreed " +
                             $"{conventions.PropertyCase} convention.");
            }
        }
    }

    private static void ValidateEmbedded(
        ValidationResult r,
        List<CdcSinkEmbeddedTableConfig>? embedded,
        string? parentTable,
        List<string>? parentPrimaryKeys,
        string owner,
        MigrationPlan plan,
        SchemaCatalog schema,
        int depth)
    {
        foreach (var e in embedded ?? new List<CdcSinkEmbeddedTableConfig>())
        {
            var what = $"embedded '{e.SourceTableName}' in {owner}";

            if (string.IsNullOrWhiteSpace(e.PropertyName))
                r.Errors.Add($"{what}: PropertyName is required.");
            if (string.IsNullOrWhiteSpace(e.SourceTableName))
                r.Errors.Add($"{owner}: an embedded table has no SourceTableName.");

            if (e.JoinColumns is null || e.JoinColumns.Count == 0)
            {
                r.Errors.Add($"{what}: JoinColumns is required - without it CDC Sink cannot tell which " +
                             $"document a changed row belongs to.");
            }
            else if (parentPrimaryKeys is { Count: > 0 } && e.JoinColumns.Count != parentPrimaryKeys.Count)
            {
                r.Warnings.Add($"{what}: {e.JoinColumns.Count} join column(s) against a parent key of " +
                               $"{parentPrimaryKeys.Count} column(s) on '{parentTable}'.");
            }

            if (schema.Knows(e.SourceTableName))
            {
                foreach (var jc in e.JoinColumns ?? new List<string>())
                {
                    if (schema.HasColumn(e.SourceTableName, jc) == false)
                        r.Errors.Add($"{what}: join column '{jc}' does not exist on '{e.SourceTableName}'.");
                }
            }

            ValidateColumns(r, e.SourceTableName, e.PrimaryKeyColumns, e.Columns, schema, plan.Conventions, what);

            if (e.Type == CdcSinkRelationType.Value && e.Columns is { Count: > 6 })
            {
                r.Warnings.Add($"{what}: a Value relation copying {e.Columns.Count} columns is close to " +
                               $"duplicating the row - a link is usually the better shape.");
            }

            if (depth >= 3)
                r.Warnings.Add($"{what}: nesting depth {depth}. Deeply nested embeds get expensive to rewrite.");

            foreach (var linked in e.LinkedTables ?? new List<CdcSinkLinkedTableConfig>())
                ValidateLinkedCore(r, linked, plan, schema, what);

            ValidateEmbedded(r, e.EmbeddedTables, e.SourceTableName, e.PrimaryKeyColumns,
                             $"{owner}.{e.PropertyName}", plan, schema, depth + 1);
        }
    }

    private static void ValidateLinked(
        ValidationResult r,
        CdcSinkLinkedTableConfig linked,
        CdcSinkTableConfig parent,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        ValidateLinkedCore(r, linked, plan, schema, $"linked '{linked.SourceTableName}' in {parent.CollectionName}");

        if (schema.Knows(parent.SourceTableName))
        {
            foreach (var jc in linked.JoinColumns ?? new List<string>())
            {
                if (schema.HasColumn(parent.SourceTableName, jc) == false)
                {
                    r.Errors.Add($"linked '{linked.SourceTableName}' in {parent.CollectionName}: join column " +
                                 $"'{jc}' does not exist on the parent table '{parent.SourceTableName}'.");
                }
            }
        }
    }

    private static void ValidateLinkedCore(
        ValidationResult r,
        CdcSinkLinkedTableConfig linked,
        MigrationPlan plan,
        SchemaCatalog schema,
        string what)
    {
        if (string.IsNullOrWhiteSpace(linked.PropertyName))
            r.Errors.Add($"{what}: PropertyName is required.");
        if (string.IsNullOrWhiteSpace(linked.LinkedCollectionName))
            r.Errors.Add($"{what}: LinkedCollectionName is required - it forms the referenced document ID.");
        if (linked.JoinColumns is null || linked.JoinColumns.Count == 0)
            r.Errors.Add($"{what}: JoinColumns is required.");

        // A reference into a collection nobody is producing is a dangling ID. Warn, don't block:
        // the target may be registered in a later call in the same turn.
        if (string.IsNullOrWhiteSpace(linked.LinkedCollectionName) == false &&
            plan.TryGet(linked.LinkedCollectionName, out _) == false)
        {
            r.Warnings.Add($"{what}: nothing in the plan produces the '{linked.LinkedCollectionName}' " +
                           $"collection yet, so these references will dangle until it is registered.");
        }
    }

    /// <summary>
    /// A total derived from child rows has to be patched from the child mapping. A patch on the
    /// root only runs when the root row itself changes, so the value goes stale the moment a line
    /// is added. This catches the common version of that mistake.
    /// </summary>
    private static void ValidateDerivedValuePlacement(ValidationResult r, CdcSinkTableConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.Patch))
            return;

        foreach (var e in config.EmbeddedTables ?? new List<CdcSinkEmbeddedTableConfig>())
        {
            if (string.IsNullOrWhiteSpace(e.PropertyName))
                continue;

            var referencesChild = Regex.IsMatch(config.Patch, $@"\bthis\.{Regex.Escape(e.PropertyName)}\b");
            if (referencesChild && string.IsNullOrWhiteSpace(e.Patch))
            {
                r.Errors.Add($"{config.CollectionName}: the root Patch reads this.{e.PropertyName}, which is " +
                             $"maintained by the embedded '{e.SourceTableName}' mapping. A root patch only runs " +
                             $"when the root row changes, so this value goes stale. Move it to the " +
                             $"'{e.SourceTableName}' mapping's Patch.");
            }
        }
    }

    /// <summary>
    /// The same rows embedded in two documents means two copies to keep consistent. A table that
    /// is both a root collection and linked from elsewhere is fine and common - that is what a
    /// reference is for - so only embedding raises this.
    /// </summary>
    private static void ValidateTableOwnership(
        ValidationResult r,
        string collection,
        CdcSinkTableConfig config,
        MigrationPlan plan)
    {
        var usage = plan.TableUsage();

        void Check(string table, bool embeddingHere)
        {
            if (string.IsNullOrWhiteSpace(table) || usage.TryGetValue(table, out var existing) == false)
                return;

            // This call replaces whatever the collection already had, so its own entries do not count.
            var elsewhere = existing
                .Where(u => string.Equals(u.Collection, collection, StringComparison.OrdinalIgnoreCase) == false)
                .ToArray();

            if (elsewhere.Length == 0)
                return;

            if (embeddingHere || elsewhere.Any(u => u.Kind == TableUseKind.Embedded))
            {
                r.Warnings.Add($"table '{table}' is also used as: " +
                               $"{string.Join("; ", elsewhere.Select(u => u.ToString()))}. Embedding the same rows " +
                               $"in more than one document means more than one copy to keep consistent - intended?");
            }
        }

        Check(config.SourceTableName, embeddingHere: false);
        foreach (var e in config.EmbeddedTables ?? new List<CdcSinkEmbeddedTableConfig>())
            Check(e.SourceTableName, embeddingHere: true);
    }

    private static bool MatchesCase(string name, PropertyCase convention) => convention switch
    {
        PropertyCase.SnakeCase => Regex.IsMatch(name, "^[a-z][a-z0-9]*(_[a-z0-9]+)*$"),
        PropertyCase.CamelCase => Regex.IsMatch(name, "^[a-z][A-Za-z0-9]*$"),
        PropertyCase.PascalCase => Regex.IsMatch(name, "^[A-Z][A-Za-z0-9]*$"),
        _ => true
    };
}

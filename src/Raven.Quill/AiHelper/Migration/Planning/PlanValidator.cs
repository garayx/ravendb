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

        ValidateTableNamesAreBare(r, config);

        var rootTable = MigrationPlan.Qualify(config.SourceTableSchema, config.SourceTableName);

        ValidateColumns(r, rootTable, config.PrimaryKeyColumns, config.Columns, schema, plan.Conventions, "root");

        foreach (var linked in config.LinkedTables ?? new List<CdcSinkLinkedTableConfig>())
            ValidateLinked(r, linked, config, rootTable, plan, schema);

        ValidateEmbedded(r, config.EmbeddedTables, rootTable, config.PrimaryKeyColumns,
                         collection, config.SourceTableSchema, plan, schema, depth: 1);

        ValidateDerivedValuePlacement(r, config);
        ValidateTableOwnership(r, collection, config, plan);

        if (r.Errors.Count == 0)
            ValidateAsTask(r, config);

        return r;
    }

    /// <summary>
    /// A schema written into the table name resolves here, but the CDC runtime reads the name as one
    /// identifier - and an embedded table's inherited schema is prefixed to it on apply. The schema has
    /// to go in SourceTableSchema.
    /// </summary>
    private static void ValidateTableNamesAreBare(ValidationResult r, CdcSinkTableConfig config)
    {
        void Check(string? table, string what)
        {
            if (table is null || table.Contains('.') == false)
                return;

            var split = table.LastIndexOf('.');
            r.Errors.Add($"{what} SourceTableName '{table}' includes a schema. Set SourceTableSchema to " +
                         $"'{table[..split]}' and SourceTableName to '{table[(split + 1)..]}'.");
        }

        Check(config.SourceTableName, "root");

        foreach (var linked in config.LinkedTables ?? [])
            Check(linked.SourceTableName, "linked");

        CdcSinkConfiguration.ForEachEmbeddedTable(config.EmbeddedTables, embedded =>
        {
            Check(embedded.SourceTableName, "embedded");

            foreach (var linked in embedded.LinkedTables ?? [])
                Check(linked.SourceTableName, "linked");
        });
    }

    /// <summary>
    /// Apply runs the task's own validation over the assembled configuration. Running it here too,
    /// over this one table, means a mapping the plan accepts cannot fail there later - by then the
    /// model is no longer in the loop to fix it. It runs last, as a backstop: the checks above
    /// explain the same problems better, in terms of the schema the model was given.
    /// </summary>
    private static void ValidateAsTask(ValidationResult r, CdcSinkTableConfig config)
    {
        var task = new CdcSinkConfiguration { Name = "plan", ConnectionStringName = "plan", Tables = [config] };

        if (task.Validate(out var errors, validateName: false, validateConnection: false) == false)
            r.Errors.AddRange(errors);
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

        if (schema.IsAmbiguous(table))
        {
            r.Errors.Add($"{what} '{table}': the source schema declares more than one table with this name " +
                         $"({string.Join(", ", schema.Candidates(table))}). Set SourceTableSchema to say which one.");
        }

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
        string? defaultSchema,
        MigrationPlan plan,
        SchemaCatalog schema,
        int depth)
    {
        foreach (var e in embedded ?? new List<CdcSinkEmbeddedTableConfig>())
        {
            // An embedded entry that names no schema of its own belongs to its root's.
            var embeddedTable = MigrationPlan.Qualify(
                string.IsNullOrWhiteSpace(e.SourceTableSchema) ? defaultSchema : e.SourceTableSchema,
                e.SourceTableName);

            var what = $"embedded '{embeddedTable}' in {owner}";

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

            if (schema.Knows(embeddedTable))
            {
                foreach (var jc in e.JoinColumns ?? new List<string>())
                {
                    if (schema.HasColumn(embeddedTable, jc) == false)
                        r.Errors.Add($"{what}: join column '{jc}' does not exist on '{embeddedTable}'.");
                }
            }

            ValidateColumns(r, embeddedTable, e.PrimaryKeyColumns, e.Columns, schema, plan.Conventions, what);

            if (e.Type == CdcSinkRelationType.Value && e.Columns is { Count: > 6 })
            {
                r.Warnings.Add($"{what}: a Value relation copying {e.Columns.Count} columns is close to " +
                               $"duplicating the row - a link is usually the better shape.");
            }

            if (depth >= 3)
                r.Warnings.Add($"{what}: nesting depth {depth}. Deeply nested embeds get expensive to rewrite.");

            foreach (var linked in e.LinkedTables ?? new List<CdcSinkLinkedTableConfig>())
                ValidateLinkedCore(r, linked, plan, schema, what);

            ValidateEmbedded(r, e.EmbeddedTables, embeddedTable, e.PrimaryKeyColumns,
                             $"{owner}.{e.PropertyName}", defaultSchema, plan, schema, depth + 1);
        }
    }

    private static void ValidateLinked(
        ValidationResult r,
        CdcSinkLinkedTableConfig linked,
        CdcSinkTableConfig parent,
        string? parentTable,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        ValidateLinkedCore(r, linked, plan, schema, $"linked '{linked.SourceTableName}' in {parent.CollectionName}");

        if (schema.Knows(parentTable))
        {
            foreach (var jc in linked.JoinColumns ?? new List<string>())
            {
                if (schema.HasColumn(parentTable, jc) == false)
                {
                    r.Errors.Add($"linked '{linked.SourceTableName}' in {parent.CollectionName}: join column " +
                                 $"'{jc}' does not exist on the parent table '{parentTable}'.");
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

        void Check(string? table, bool embeddingHere)
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

        // Qualified to match how the plan keys its usage: same schema, same table, or it is a
        // different table that happens to share a name.
        Check(MigrationPlan.Qualify(config.SourceTableSchema, config.SourceTableName), embeddingHere: false);

        foreach (var e in config.EmbeddedTables ?? new List<CdcSinkEmbeddedTableConfig>())
        {
            Check(
                MigrationPlan.Qualify(e.SourceTableSchema ?? config.SourceTableSchema, e.SourceTableName),
                embeddingHere: true);
        }
    }

    private static bool MatchesCase(string name, PropertyCase convention) => convention switch
    {
        PropertyCase.SnakeCase => Regex.IsMatch(name, "^[a-z][a-z0-9]*(_[a-z0-9]+)*$"),
        PropertyCase.CamelCase => Regex.IsMatch(name, "^[a-z][A-Za-z0-9]*$"),
        PropertyCase.PascalCase => Regex.IsMatch(name, "^[A-Z][A-Za-z0-9]*$"),
        _ => true
    };
}

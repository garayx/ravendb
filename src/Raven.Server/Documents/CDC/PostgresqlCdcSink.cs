using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.CDC.Stats;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CDC;

public sealed class PostgresqlCdcSink : CdcSinkProcess
{
    public PostgresqlCdcSink(CdcSinkConfiguration configuration, CdcSinkProcessState processState, DocumentDatabase database, string tag) : base(configuration, database, tag)
    {
        _testTables = configuration.Settings.Collections;

        LastLsn = new NpgsqlTypes.NpgsqlLogSequenceNumber(processState.LastLsn);
        CdcConfigId = CdcSinkConfiguration.RavenConfigIdPrefix + CdcSinkConfiguration.ComputeTablesHash(_testTables);
        BuildNestedTableLookup();
    }

    public string CdcConfigId;

    public List<Collection2> _testTables { get; set; }

    public NpgsqlTypes.NpgsqlLogSequenceNumber LastLsn { get; set; }

    private IDatabaseDriver _dbDriver;
    private DatabaseSchema _schema;

    /// <summary>
    /// Maps child table name (lowercase) to its nested collection metadata.
    /// When a CDC message arrives for a table in this lookup, we know it's a nested
    /// collection that should be embedded into its parent document.
    /// </summary>
    private Dictionary<string, NestedTableInfo> _nestedTableLookup;

    private sealed class NestedTableInfo
    {
        /// <summary>The parent Collection2 that owns this nested collection.</summary>
        public Collection2 ParentCollection;

        /// <summary>The nested collection definition.</summary>
        public NestedCollection2 NestedCollection;
    }

    private void BuildNestedTableLookup()
    {
        _nestedTableLookup = new Dictionary<string, NestedTableInfo>(StringComparer.OrdinalIgnoreCase);

        foreach (var parentTable in _testTables)
        {
            if (parentTable.NestedCollections == null)
                continue;

            foreach (var nested in parentTable.NestedCollections)
            {
                _nestedTableLookup[nested.SourceTableName] = new NestedTableInfo
                {
                    ParentCollection = parentTable,
                    NestedCollection = nested
                };
            }
        }
    }

    /// <summary>
    /// Returns all table names that need to be in the PostgreSQL publication:
    /// the top-level tables plus any nested child tables.
    /// </summary>
    private IEnumerable<string> GetAllPublicationTableNames()
    {
        foreach (var table in _testTables)
        {
            yield return table.SourceTableName;

            if (table.NestedCollections != null)
            {
                foreach (var nested in table.NestedCollections)
                    yield return nested.SourceTableName;
            }
        }
    }

    private async Task EnsureReplicationSetupAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using (var cmd = new NpgsqlCommand(
                         $"SELECT 1 FROM pg_publication WHERE pubname = @pubName",
                         conn))
        {
            cmd.Parameters.AddWithValue("pubName", Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName);
            var exists = await cmd.ExecuteScalarAsync(cancellationToken);

            if (exists == null)
            {
                var allTables = GetAllPublicationTableNames().Distinct(StringComparer.OrdinalIgnoreCase);
                await using var createCmd = new NpgsqlCommand(
                    $"CREATE PUBLICATION {Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName} FOR TABLE {string.Join(", ", allTables.Select(x => $"\"{x}\""))};",
                    conn);
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        // Terminate any active backend that may still be holding our replication slot
        // (e.g., from a previous process instance that was stopped when the database was disabled)
        try
        {
            await using var terminateCmd = new NpgsqlCommand(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = @slotName AND active_pid IS NOT NULL",
                conn);
            terminateCmd.Parameters.AddWithValue("slotName", Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName);
            await terminateCmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            // ignore errors
        }

        try
        {
            await using (var cmd = new NpgsqlCommand(
                             $"SELECT pg_create_logical_replication_slot('{Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName}', 'pgoutput');",
                             conn))
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (PostgresException ex) when (ex.SqlState == "42710")
        {
            // ignore if the replication slot already exists
        }
    }

    protected override void Initialize()
    {
        _dbDriver ??= DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        _schema ??= _dbDriver.FindSchema();
    }

    protected override async Task<ICdcSinkConsumer> CreateConsumerAsync()
    {
        await EnsureReplicationSetupAsync(CancellationToken);

        var conn = new LogicalReplicationConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);

        await conn.Open(CancellationToken);

        var replicationStream = conn.StartReplication(
            new PgOutputReplicationSlot(Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName),
            new PgOutputReplicationOptions(Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName, PgOutputProtocolVersion.V1),
            CancellationToken,
            LastLsn);

        return new CdcPostgresSqlSinkConsumer(conn, replicationStream);
    }

    private async Task CleanupAllReplicationSlotsAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand("""
                                                SELECT pg_terminate_backend(active_pid)
                                                FROM pg_replication_slots
                                                WHERE active_pid IS NOT NULL;

                                                SELECT pg_drop_replication_slot(slot_name)
                                                FROM pg_replication_slots
                                                WHERE slot_type = 'logical';
                                                """, conn);
        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == "42704")
        {
        }
    }

    private async Task CleanupReplicationSlotsBySlotsNameAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        // Step 1: Terminate any active backend holding the slot
        try
        {
            await using var terminateCmd = new NpgsqlCommand(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = @slotName AND active_pid IS NOT NULL",
                conn);
            terminateCmd.Parameters.AddWithValue("slotName", Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName);
            await terminateCmd.ExecuteNonQueryAsync(cancellationToken);

            // Give PostgreSQL time to fully release the slot after terminating the backend
            await Task.Delay(1000, cancellationToken);
        }
        catch
        {
            // ignore errors from terminate
        }

        // Step 2: Drop the replication slot
        try
        {
            await using var dropCmd = new NpgsqlCommand(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = @slotName",
                conn);
            dropCmd.Parameters.AddWithValue("slotName", Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName);
            await dropCmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == "42704")
        {
            // slot doesn't exist, ignore
        }
    }

    protected override async Task HandleInitialLoadAsync()
    {
        if (LastLsn > ZeroLsn)
        {
            return;
        }

        var config = GetCdcConfiguration();
        if (config.Tables.All(t => t.InitialLoadCompleted))
            return;

        foreach (var table in config.Tables)
        {
            if (table.InitialLoadCompleted)
                continue;

            config = await ProcessTableInitialLoad(table, config, CancellationToken);
        }
        //TODO: egor inline this with ProcessTableInitialLoad and only after nested fill finish, mark as InitialLoadCompleted

        // Phase 2: For each parent table that has nested collections, query the child tables
        // and embed rows into the appropriate parent documents via the TxMerger
        foreach (var table in config.Tables)
        {
            if (table.NestedCollections == null || table.NestedCollections.Count == 0)
                continue;

            foreach (var nested in table.NestedCollections)
            {
                await ProcessNestedCollectionInitialLoad(table, nested, CancellationToken);
            }
        }
    }

    private async Task ProcessNestedCollectionInitialLoad(Collection2 parentTable, NestedCollection2 nested, CancellationToken cancellationToken)
    {
        var childTableSchema = _schema.GetTable(nested.SourceTableSchema, nested.SourceTableName);
        var childSpecialColumns = _schema.FindSpecialColumns(nested.SourceTableSchema, nested.SourceTableName);

        // Ensure join columns (FK) are treated as special columns so their values
        // are available in SpecialColumnsValues for parent document ID resolution
        foreach (var joinCol in nested.JoinColumns)
        {
            childSpecialColumns.Add(joinCol);
        }

        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        var q = $"SELECT * FROM \"{nested.SourceTableName}\";";
        await using var selectCmd = new NpgsqlCommand(q, conn);
        await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);

        var nestedBatch = new List<CdcChangeItem>();
        int batchSize = Configuration.Settings.BatchSize;

        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var childDoc = new SqlMigrationDocument
                {
                    Object = nested.ColumnsMapping != null && nested.ColumnsMapping.Count > 0
                        ? GenericDatabaseMigrator.ExtractFromReader(reader, nested.ColumnsMapping)
                        : new DynamicJsonValue(),
                    Attachments = new Dictionary<string, byte[]>(),
                    SpecialColumnsValues = GenericDatabaseMigrator.ExtractFromReader(reader, childSpecialColumns),
                };

                // Extract the FK values that reference the parent
                var parentPkValues = new object[nested.JoinColumns.Count];
                for (int i = 0; i < nested.JoinColumns.Count; i++)
                {
                    parentPkValues[i] = childDoc.SpecialColumnsValues[nested.JoinColumns[i]];
                }

                var parentDocId = GenericDatabaseMigrator.GenerateDocumentId(parentTable.Name, parentPkValues);
                if (parentDocId == null)
                    continue;

                // Include the child's PK columns in the nested object so we can identify items later
                foreach (var pkCol in childTableSchema.PrimaryKeyColumns)
                {
                    var val = childDoc.SpecialColumnsValues[pkCol];
                    if (val != null)
                    {
                        var propName = char.ToUpper(pkCol[0]) + pkCol.Substring(1);
                        childDoc.Object[propName] = val;
                    }
                }

                var nestedDoc = context.ReadObject(childDoc.Object, $"nested/{nested.SourceTableName}");
                var nestedItemKey = new Dictionary<string, object>();
                foreach (var pkCol in childTableSchema.PrimaryKeyColumns)
                {
                    var propName = char.ToUpper(pkCol[0]) + pkCol.Substring(1);
                    if (nestedDoc.TryGet(propName, out object val))
                        nestedItemKey[pkCol] = val;
                }

                nestedBatch.Add(new CdcChangeItem
                {
                    ChangeType = CdcChangeType.NestedPut,
                    ParentDocumentId = parentDocId,
                    NestedPropertyName = nested.Name,
                    Document = nestedDoc,
                    NestedItemKey = nestedItemKey
                });

                if (nestedBatch.Count >= batchSize)
                {
                    var command = new Commands.BatchCdcSinkScriptCommand(nestedBatch, initialLoad: true);
                    Database.TxMerger.EnqueueSync(command);
                    nestedBatch = new List<CdcChangeItem>();
                }
            }

            // flush the remaining items
            if (nestedBatch.Count > 0)
            {
                var command = new Commands.BatchCdcSinkScriptCommand(nestedBatch, initialLoad: true);
                Database.TxMerger.EnqueueSync(command);
            }
        }
    }

    private async Task<Config> ProcessTableInitialLoad(Collection2 table, Config config, CancellationToken cancellationToken)
    {
        var tableSchema = _schema.GetTable(table.SourceTableSchema, table.SourceTableName);
        var specialColumns = _schema.FindSpecialColumns(table.SourceTableSchema, table.SourceTableName);
        var keyColumns = tableSchema.PrimaryKeyColumns.ToArray();
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var reader = await InitialLoadQuery(table, keyColumns, conn, cancellationToken);

        var batch = new List<CdcChangeItem>(Configuration.Settings.BatchSize);

        Task lastBatch = Task.CompletedTask;
        var references = new List<ReferenceInformation>();

        var hasRows = false;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                hasRows = true;
                var doc = new SqlMigrationDocument
                {
                    Object = GenericDatabaseMigrator.ExtractFromReader(reader, table.ColumnsMapping),
                    Attachments = new Dictionary<string, byte[]>(),
                    SpecialColumnsValues = GenericDatabaseMigrator.ExtractFromReader(reader, specialColumns),
                };

                var id = GenericDatabaseMigrator.GenerateDocumentId(table.Name, GenericDatabaseMigrator.GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));
                doc.SetCollectionAndId(table.Name, id);

                GenericDatabaseMigrator.FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);

                // Initialize empty arrays for nested collections
                if (table.NestedCollections != null)
                {
                    foreach (var nested in table.NestedCollections)
                    {
                        doc.Object[nested.Name] = new DynamicJsonArray();
                    }
                }

                var docBlittable = doc.ToBlittable(context);

                batch.Add(new CdcChangeItem()
                {
                    ChangeType = CdcChangeType.Put,
                    Document = docBlittable,
                    Id = id
                });

                if (batch.Count >= Configuration.Settings.BatchSize)
                {
                    var lastKeyValues = keyColumns.Select(col => reader[col]?.ToString() ?? "").ToList();
                    await lastBatch;
                    config.Tables = config.Tables.Select(t =>
                    {
                        if (t.Name == table.Name)
                        {
                            t.LastKeyValues = lastKeyValues;
                        }

                        return t;
                    }).ToList();

                    var b = batch.ToList();
                    lastBatch = Task.Run(() =>
                    {
                        var command = new Commands.BatchCdcSinkScriptCommand(b, config, initialLoad: true);
                        Database.TxMerger.EnqueueSync(command);
                    }, cancellationToken);

                    batch = [];
                }
            }

            // Flush the remaining batch (or an empty batch for config persistence).
            // This also handles: exact batch boundary (batch empty but lastBatch running),
            // and empty tables (no rows at all).
            await lastBatch;
            config.Tables = config.Tables.Select(t =>
            {
                if (t.Name == table.Name)
                {
                    t.LastKeyValues = [];
                    t.InitialLoadCompleted = true;
                }

                return t;
            }).ToList();

            var finalCommand = new Commands.BatchCdcSinkScriptCommand(batch, config, initialLoad: true);
            Database.TxMerger.EnqueueSync(finalCommand);
        }

        return config;
    }

    public class Config : IDynamicJson
    {
        public Config()
        {
            // for serializer
        }

        public Config(ulong lastLsn, List<Collection2> tables, string cdcConfigId)
        {
            LastLsn = lastLsn;
            Tables = tables;
            CdcConfigId = cdcConfigId;
        }

        public ulong LastLsn { get; set; }
        public List<Collection2> Tables { get; set; }
        public string CdcConfigId { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastLsn)] = LastLsn,
                [nameof(Tables)] = new DynamicJsonArray(Tables.Select(t => t.ToJson())),
                [nameof(CdcConfigId)] = CdcConfigId
            };
        }
    }

    private Config GetCdcConfiguration()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {

            BlittableJsonReaderObject data = null;
            try
            {
                data = Database.DocumentsStorage.Get(context, CdcConfigId)?.Data;
            }
            catch (DocumentConflictException e)
            {
                throw new InvalidDataException("Failed to fetch CDC document due to a conflict on the document. " +
                                               "This shouldn't happen, since it this conflict should've been resolved during replication. " +
                                               "This exception should not happen and is likely a bug.", e);
            }

            if (data == null)
            {
                return new Config(0, _testTables, CdcConfigId);
            }
            else
            {
                return JsonDeserializationServer.PostgresqlCdcSinkConfig(data);
            }
        }
    }

    private async Task<string[]> GetTableKeyColumns(string tableName, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        var query = @"
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)";

        await using var cmd = new NpgsqlCommand(query, conn);
        cmd.Parameters.AddWithValue(tableName);

        var keyColumns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            keyColumns.Add(reader.GetString(0));
        }

        return keyColumns.ToArray();
    }


    private async Task<NpgsqlDataReader> InitialLoadQuery(Collection2 table, string[] keyColumns, NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        var query = $"SELECT * FROM \"{table.SourceTableName}\"";
        var parameters = new Dictionary<string, object>();

        if (table.LastKeyValues.Count > 0)
        {
            // Get column types for proper parameter conversion
            var columnTypes = await GetColumnTypes(table.SourceTableName, keyColumns, conn, cancellationToken);

            var whereConditions = new List<string>();
            for (int i = 0; i < keyColumns.Length; i++)
            {
                var paramName = $"@key{i}";
                if (i == keyColumns.Length - 1)
                {
                    // Last key column: >=
                    whereConditions.Add($"{keyColumns[i]} >= {paramName}");
                }
                else
                {
                    // Previous columns: =
                    whereConditions.Add($"{keyColumns[i]} = {paramName}");
                }

                var stringValue = table.LastKeyValues[i];
                var columnType = columnTypes[keyColumns[i]];
                parameters[paramName] = GenericDatabaseMigrator.ExtractReplicationValue(stringValue, columnType);
            }
            query += " WHERE " + string.Join(" AND ", whereConditions);
        }

        if (keyColumns.Length == 0)
        {
            // TODO: egor we might have 0 PK
            query += ";";
        }
        else
        {
            query += " ORDER BY " + string.Join(", ", keyColumns);
        }

        var cmd = new NpgsqlCommand(query, conn);
        foreach (var param in parameters)
        {
            cmd.Parameters.AddWithValue(param.Key, param.Value);
        }

        return await cmd.ExecuteReaderAsync(cancellationToken);
    }

    private async Task<Dictionary<string, string>> GetColumnTypes(string tableName, string[] columnNames, NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        var columnTypes = new Dictionary<string, string>();

        var query = @"
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = @tableName
            AND column_name = ANY(@columns)";

        await using var cmd = new NpgsqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@tableName", tableName);
        cmd.Parameters.AddWithValue("@columns", columnNames);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var columnName = reader.GetString(0);
            var dataType = reader.GetString(1);
            columnTypes[columnName] = dataType;
        }

        return columnTypes;
    }

    private static readonly NpgsqlLogSequenceNumber ZeroLsn = new NpgsqlLogSequenceNumber(0);
    protected static CdcBatchResult EmptyCdcBatch = new CdcBatchResult() { Status = CdcBatchStatus.EmptyBatch };
    protected static CdcBatchResult ContinueCdcBatch = new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };

    protected override async Task<CdcBatchResult> ProcessBatchItemAsync(DocumentsOperationContext context, PgOutputReplicationMessage message, List<CdcChangeItem> messages, CdcSinkStatsScope readScope)
    {
        if (message == null)
            return EmptyCdcBatch;

        switch (message)
        {
            case InsertMessage insert:
                {
                    if (_nestedTableLookup.TryGetValue(insert.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, insert.Relation, insert.NewRow, nestedInfo, CdcChangeType.NestedPut);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, doc) = await GetRowData(context, insert.Relation, insert.NewRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };
                }
            case DefaultUpdateMessage defaultUpdateMessage:
                {
                    if (_nestedTableLookup.TryGetValue(defaultUpdateMessage.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, defaultUpdateMessage.Relation, defaultUpdateMessage.NewRow, nestedInfo, CdcChangeType.NestedPut);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, doc) = await GetRowData(context, defaultUpdateMessage.Relation, defaultUpdateMessage.NewRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };
                }
            case FullUpdateMessage fullUpdate:
                {
                    if (_nestedTableLookup.TryGetValue(fullUpdate.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, fullUpdate.Relation, fullUpdate.NewRow, nestedInfo, CdcChangeType.NestedPut);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, doc) = await GetRowData(context, fullUpdate.Relation, fullUpdate.NewRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };
                }
            case IndexUpdateMessage indexUpdate:
                {
                    if (_nestedTableLookup.TryGetValue(indexUpdate.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, indexUpdate.Relation, indexUpdate.NewRow, nestedInfo, CdcChangeType.NestedPut);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, doc) = await GetRowData(context, indexUpdate.Relation, indexUpdate.NewRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };
                }
            case UpdateMessage update:
                {
                    if (_nestedTableLookup.TryGetValue(update.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, update.Relation, update.NewRow, nestedInfo, CdcChangeType.NestedPut);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, doc) = await GetRowData(context, update.Relation, update.NewRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentsSent };
                }
            case KeyDeleteMessage keyDel:
                {
                    if (_nestedTableLookup.TryGetValue(keyDel.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, keyDel.Relation, keyDel.Key, nestedInfo, CdcChangeType.NestedDelete);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, _) = await GetRowData(context, keyDel.Relation, keyDel.Key);
                        messages.Add(new CdcChangeItem { Id = id, Document = null, ChangeType = CdcChangeType.Delete });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentDeleted };
                }
            case FullDeleteMessage fullDel:
                {
                    if (_nestedTableLookup.TryGetValue(fullDel.Relation.RelationName, out var nestedInfo))
                    {
                        var item = await BuildNestedChangeItem(context, fullDel.Relation, fullDel.OldRow, nestedInfo, CdcChangeType.NestedDelete);
                        if (item != null)
                            messages.Add(item);
                    }
                    else
                    {
                        var (id, _) = await GetRowData(context, fullDel.Relation, fullDel.OldRow);
                        messages.Add(new CdcChangeItem { Id = id, Document = null, ChangeType = CdcChangeType.Delete });
                    }
                    readScope.RecordReadMessage();
                    return new CdcBatchResult() { Status = CdcBatchStatus.DocumentDeleted };
                }
            case BeginMessage:
                return ContinueCdcBatch;

            case CommitMessage commit:
                return new CdcBatchResult()
                {
                    Status = CdcBatchStatus.Commit,
                    LastLsn = commit.CommitLsn
                };
            //TODO: egor we don't care , should be made by user, we simply ignore the new columns, if column was removed? put null in the document?
            case RelationMessage relationMessage:
                _schema = _dbDriver.FindSchema();
                return ContinueCdcBatch;

            case LogicalDecodingMessage logicalDecoding:
                return ContinueCdcBatch;

            case TruncateMessage truncateMessage:
                return ContinueCdcBatch;

            //TODO: egor we don't care , same as relationMessage
            case TypeMessage typeMessage:
                _schema = _dbDriver.FindSchema();
                return ContinueCdcBatch;

            default:
                throw new InvalidOperationException($"Unsupported message type: {message.GetType().Name}");
        }
    }

    /// <summary>
    /// Builds a CdcChangeItem for a nested collection operation (insert/update/delete on a child table).
    /// The item carries the parent document ID and the nested data, so the batch command can
    /// load the parent document and modify its nested array.
    /// </summary>
    private async Task<CdcChangeItem> BuildNestedChangeItem(
        DocumentsOperationContext context,
        RelationMessage relation,
        ReplicationTuple row,
        NestedTableInfo nestedInfo,
        CdcChangeType changeType)
    {
        var nested = nestedInfo.NestedCollection;
        var parent = nestedInfo.ParentCollection;

        var childTableSchema = _schema.GetTable(nested.SourceTableSchema, nested.SourceTableName);
        var childSpecialColumns = _schema.FindSpecialColumns(nested.SourceTableSchema, nested.SourceTableName);

        // Ensure join columns (FK) are treated as special columns so their values
        // are available in SpecialColumnsValues for parent document ID resolution
        foreach (var joinCol in nested.JoinColumns)
        {
            childSpecialColumns.Add(joinCol);
        }

        // Extract all columns from the CDC row
        var childColumnsMapping = nested.ColumnsMapping != null && nested.ColumnsMapping.Count > 0
            ? nested.ColumnsMapping
            : new Dictionary<string, string>();

        // We need to read ALL columns (both mapped and special) from the replication tuple
        var allColumnsToRead = new Dictionary<string, string>(childColumnsMapping);
        foreach (var specialCol in childSpecialColumns)
        {
            if (allColumnsToRead.ContainsKey(specialCol) == false)
                allColumnsToRead[specialCol] = specialCol;
        }

        // Ensure special columns (PKs, FKs) are NOT in allColumnsToRead so ExtractFromReader
        // puts them into SpecialColumnsValues, not Document. The else-if in ExtractFromReader
        // only processes columnNames (specialColumns) when the column is NOT in tableColumnsMapping.
        foreach (var specialCol in childSpecialColumns)
        {
            if (childColumnsMapping.ContainsKey(specialCol) == false)
                allColumnsToRead.Remove(specialCol);
        }

        var doc = await GenericDatabaseMigrator.ExtractFromReader(row, allColumnsToRead, childSpecialColumns);

        // Determine the parent document ID from the join columns (FK values)
        var parentPkValues = new object[nested.JoinColumns.Count];
        for (int i = 0; i < nested.JoinColumns.Count; i++)
        {
            parentPkValues[i] = doc.SpecialColumnsValues[nested.JoinColumns[i]];
        }

        var parentTableSchema = _schema.GetTable(parent.SourceTableSchema, parent.SourceTableName);
        var parentDocId = GenericDatabaseMigrator.GenerateDocumentId(parent.Name, parentPkValues);
        if (parentDocId == null)
            return null;

        // Build the nested item key from the child's PK columns
        var nestedItemKey = new Dictionary<string, object>();
        foreach (var pkCol in childTableSchema.PrimaryKeyColumns)
        {
            nestedItemKey[pkCol] = doc.SpecialColumnsValues[pkCol];
        }

        BlittableJsonReaderObject nestedDoc = null;
        if (changeType == CdcChangeType.NestedPut)
        {
            // Build the nested object with mapped columns + PK columns
            var nestedObj = new DynamicJsonValue();

            // Add mapped column values
            if (nested.ColumnsMapping != null)
            {
                foreach (var kvp in nested.ColumnsMapping)
                {
                    nestedObj[kvp.Value] = doc.Object[kvp.Value];
                }
            }

            // Add PK column values so the item can be identified later
            foreach (var pkCol in childTableSchema.PrimaryKeyColumns)
            {
                var propName = char.ToUpper(pkCol[0]) + pkCol.Substring(1);
                nestedObj[propName] = doc.SpecialColumnsValues[pkCol];
            }

            nestedDoc = context.ReadObject(nestedObj, $"nested/{nested.SourceTableName}");
        }

        return new CdcChangeItem
        {
            Id = null, // not a standalone document
            Document = nestedDoc,
            ChangeType = changeType,
            ParentDocumentId = parentDocId,
            NestedPropertyName = nested.Name,
            NestedItemKey = nestedItemKey
        };
    }

    private async Task<(string, BlittableJsonReaderObject)> GetRowData(DocumentsOperationContext context, RelationMessage relation, ReplicationTuple row)
    {
        var table = _testTables.FirstOrDefault(x => x.SourceTableName == relation.RelationName);

        if (table == null)
            throw new InvalidOperationException($"Collection2 not found for relation: {relation.RelationName}");

        var tableSchema = _schema.GetTable(table.SourceTableSchema, table.SourceTableName);
        HashSet<string> specialColumns = _schema.FindSpecialColumns(table.SourceTableSchema, table.SourceTableName);

        var doc = await GenericDatabaseMigrator.ExtractFromReader(row, table.ColumnsMapping, specialColumns);

        var id = GenericDatabaseMigrator.GenerateDocumentId(table.Name, GenericDatabaseMigrator.GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));
        doc.SetCollectionAndId(table.Name, id);

        var references = new List<ReferenceInformation>();
        GenericDatabaseMigrator.FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);

        BlittableJsonReaderObject docBlittable = doc.ToBlittable(context);

        return (id, docBlittable);
    }
}

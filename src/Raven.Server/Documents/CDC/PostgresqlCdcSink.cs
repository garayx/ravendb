using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Raven.Client.Documents.Operations.CDC;
using Raven.Server.Documents.CDC.Stats;
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
    }

    public List<Collection2> _testTables { get; set; }

    public NpgsqlTypes.NpgsqlLogSequenceNumber LastLsn { get; set; }

    private IDatabaseDriver _dbDriver;
    private DatabaseSchema _schema;

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
                await using var createCmd = new NpgsqlCommand(
                    $"CREATE PUBLICATION {Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName} FOR TABLE {string.Join(", ", _testTables.Select(x => $"\"{x.SourceTableName}\""))};",
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
            // ignore errors — slot may not exist yet or no active backend
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
            // ignore if the replication slot already exits
        }
    }

    protected override void Initialize()
    {
        _dbDriver ??= DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        _schema ??= _dbDriver.FindSchema();
    }

    protected override async Task<ICdcSinkConsumer> CreateConsumerAsync()
    {
        //TODO: egor make it async, maybe introduce CreateConsumerASync?

        await EnsureReplicationSetupAsync(CancellationToken);
        // Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName
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

        // Terminate any active backends holding our slot, then drop it
        await using var cmd = new NpgsqlCommand("""
                                                SELECT pg_terminate_backend(active_pid)
                                                FROM pg_replication_slots
                                                WHERE active_pid IS NOT NULL;

                                                SELECT pg_drop_replication_slot(slot_name)
                                                FROM pg_replication_slots
                                                WHERE slot_type = 'logical';
                                                """, conn);
        //try
        //{
        //    await using var dropCmd = new NpgsqlCommand(
        //        $"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName}';",
        //        regularConn);
        //    await dropCmd.ExecuteNonQueryAsync(CancellationToken);
        //}
        //catch { /* ignore if not exists */ }

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
         
        }
        catch (PostgresException ex) when (ex.SqlState == "42704") // undefined_object: slot does not exist
        {
            // Slot was already gone — nothing to clean up
        }
    }private async Task CleanupReplicationSlotsBySlotsNameAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        // Terminate any active backends holding our slot, then drop it
        await using var cmd = new NpgsqlCommand("""
                                                SELECT pg_terminate_backend(active_pid)
                                                FROM pg_replication_slots
                                                WHERE slot_name = @slotName
                                                  AND active_pid IS NOT NULL;

                                                SELECT pg_drop_replication_slot(slot_name)
                                                FROM pg_replication_slots
                                                WHERE slot_name = @slotName;
                                                """, conn);

        cmd.Parameters.AddWithValue("slotName", Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName);

        //try
        //{
        //    await using var dropCmd = new NpgsqlCommand(
        //        $"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName}';",
        //        regularConn);
        //    await dropCmd.ExecuteNonQueryAsync(CancellationToken);
        //}
        //catch { /* ignore if not exists */ }

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == "42704") // undefined_object: slot does not exist
        {
            // Slot was already gone — nothing to clean up
        }
    }

    protected override async Task HandleInitialLoadAsync()
    {
        if (LastLsn > ZeroLsn)
        {
            return;
        }

        try
        {
            await CleanupReplicationSlotsBySlotsNameAsync(CancellationToken);

            //DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);

            await using var conn = new LogicalReplicationConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);

            await conn.Open(CancellationToken);

            var slotOptions = await conn.CreatePgOutputReplicationSlot(
                Configuration.Connection.PostgresqlConnectionSettings.PostgresSlotName,
                // This enum is the magic key that gives us the snapshot ID
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                cancellationToken: CancellationToken);

            string snapshotName = slotOptions.SnapshotName;

            await using var regularConn = new NpgsqlConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
            await regularConn.OpenAsync(CancellationToken);

            // A snapshot must be used within a RepeatableRead transaction
            await using var tx = await regularConn.BeginTransactionAsync(IsolationLevel.RepeatableRead, CancellationToken);

            // Tell Postgres to set the transaction to our exported snapshot
            await using var setSnapshotCmd = new NpgsqlCommand($"SET TRANSACTION SNAPSHOT '{snapshotName}';", regularConn, tx);
            await setSnapshotCmd.ExecuteNonQueryAsync(CancellationToken);
            // Now query the table. This query is "frozen" at the exact moment the slot was created.
            int existingRowCount = 0;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new SqlMigrationWriter(context, Configuration.Settings.BatchSize))
            {
                foreach (var table in _testTables)
                {
                    var tableSchema = _schema.GetTable(table.SourceTableSchema, table.SourceTableName);
                    var specialColumns = _schema.FindSpecialColumns(table.SourceTableSchema, table.SourceTableName);

                    if (table.SourceTableName == "Order")
                    {

                    }
                    // await using var selectCmd = new NpgsqlCommand(dbDriver.GetSelectAllQueryForTable($"'{table.SourceTableSchema}'", $"'{table.SourceTableName}'"), regularConn, tx);
                    var q = $"SELECT * FROM \"{table.SourceTableName}\";";
                    Console.WriteLine(q);
                    await using var selectCmd = new NpgsqlCommand(q, regularConn, tx);
                    await using var reader = await selectCmd.ExecuteReaderAsync(CancellationToken);
                    var references = new List<ReferenceInformation>();
                    while (await reader.ReadAsync(CancellationToken))
                    {
                        // TODO: Map and save your historical documents here

                        var doc = new SqlMigrationDocument
                        {
                            Object = GenericDatabaseMigrator.ExtractFromReader(reader, table.ColumnsMapping),
                            Attachments = new Dictionary<string, byte[]>(),
                            SpecialColumnsValues = GenericDatabaseMigrator.ExtractFromReader(reader, specialColumns),
                        };



                        var id = GenericDatabaseMigrator.GenerateDocumentId(table.Name, GenericDatabaseMigrator.GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));
                        doc.SetCollectionAndId(table.Name, id);


                        GenericDatabaseMigrator.FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);
                        // var docBlittable = patcher.Patch(doc.ToBlittable(context));
                        var docBlittable = doc.ToBlittable(context);
                        await writer.InsertDocument(docBlittable, id, doc.Attachments);


                        existingRowCount++;
                    }

                    await reader.CloseAsync();

                }
            }

            // Commit the transaction to release the snapshot lock
            await tx.CommitAsync(CancellationToken);

            LastLsn = slotOptions.ConsistentPoint;

            UpdateProcessState(new CdcSinkProcessState
            {
                ConfigurationName = Configuration.Name,
                ScriptName = Script.Name,
                NodeTag = Database.ServerStore.NodeTag,
                LastLsn = (ulong)LastLsn
            });
            //TODO: egor commit the LSN to cdc state

            Console.WriteLine($"Initial sync complete. Processed {existingRowCount} historical rows.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
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
                    var (id, doc) = await GetRowData(context, insert.Relation, insert.NewRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentsSent
                    };
                }
            case DefaultUpdateMessage defaultUpdateMessage:
                {
                    var (id, doc) = await GetRowData(context, defaultUpdateMessage.Relation, defaultUpdateMessage.NewRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentsSent
                    };
                }
            case FullUpdateMessage fullUpdate:
                {
                    //TODO: egor test
                    var (id, doc) = await GetRowData(context, fullUpdate.Relation, fullUpdate.NewRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentsSent
                    };
                }
            case IndexUpdateMessage indexUpdate:
                {
                    //TODO: egor test
                    var (id, doc) = await GetRowData(context, indexUpdate.Relation, indexUpdate.NewRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentsSent
                    };
                }
            case UpdateMessage update:
                {
                    var (id, doc) = await GetRowData(context, update.Relation, update.NewRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = doc, ChangeType = CdcChangeType.Put });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentsSent
                    };
                }
            case KeyDeleteMessage keyDel:
                {
                    var (id, _) = await GetRowData(context, keyDel.Relation, keyDel.Key);
                    messages.Add(new CdcChangeItem { Id = id, Document = null, ChangeType = CdcChangeType.Delete });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentDeleted
                    };
                }
            case FullDeleteMessage fullDel:
                {
                    var (id, _) = await GetRowData(context, fullDel.Relation, fullDel.OldRow);
                    messages.Add(new CdcChangeItem { Id = id, Document = null, ChangeType = CdcChangeType.Delete });
                    readScope.RecordReadMessage();

                    return new CdcBatchResult()
                    {
                        Status = CdcBatchStatus.DocumentDeleted
                    };
                }
            case BeginMessage: // begin tx
                return ContinueCdcBatch;

            case CommitMessage commit:

                return new CdcBatchResult()
                {
                    Status = CdcBatchStatus.Commit,
                    LastLsn = commit.CommitLsn
                };

            case RelationMessage relationMessage:
                // A RelationMessage is sent by PostgreSQL whenever the structure of a replicated table changes
                // (e.g., ALTER TABLE ADD COLUMN) or at the beginning of the replication stream for each table.
                // We must refresh our cached schema so that subsequent Insert/Update messages are decoded correctly
                // against the new column set.
                _schema = _dbDriver.FindSchema();

                return ContinueCdcBatch;

            case LogicalDecodingMessage logicalDecoding:
                // A user-defined logical decoding message emitted via pg_logical_emit_message().
                // These are application-level messages injected into the WAL stream and do not
                // correspond to any DML operation. Safe to ignore for CDC replication purposes.
                return ContinueCdcBatch;

            case TruncateMessage truncateMessage:
                // A TRUNCATE was executed on one or more replicated tables.
                // For now we skip this; a future enhancement could delete all documents
                // in the corresponding RavenDB collection.
                return ContinueCdcBatch;

            case TypeMessage typeMessage:
                // Sent when a custom PostgreSQL type used by a replicated column is created or changed.
                // Safe to ignore for standard data types; refresh schema to pick up any type changes.
                _schema = _dbDriver.FindSchema();
                return ContinueCdcBatch;

            default:
                //TODO: egor do we want to throw or log?
                throw new InvalidOperationException($"Unsupported message type: {message.GetType().Name}");
        }

    }

    private async Task<(string, BlittableJsonReaderObject)> GetRowData(DocumentsOperationContext context, RelationMessage relation, ReplicationTuple row)
    {
        var table = _testTables.FirstOrDefault(x => x.SourceTableName == relation.RelationName);

        if (table == null)
            throw new InvalidOperationException($"Table not found for relation: {relation.RelationName}");

        var tableSchema = _schema.GetTable(table.SourceTableSchema, table.SourceTableName);
        HashSet<string> specialColumns = _schema.FindSpecialColumns(table.SourceTableSchema, table.SourceTableName);

        var doc = await GenericDatabaseMigrator.ExtractFromReader(row, table.ColumnsMapping, specialColumns);


        var id = GenericDatabaseMigrator.GenerateDocumentId(table.Name, GenericDatabaseMigrator.GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));
        doc.SetCollectionAndId(table.Name, id);

        var references = new List<ReferenceInformation>();
        GenericDatabaseMigrator.FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);
        // var docBlittable = patcher.Patch(doc.ToBlittable(context));
        BlittableJsonReaderObject docBlittable = doc.ToBlittable(context);

        return (id, docBlittable);
    }
}

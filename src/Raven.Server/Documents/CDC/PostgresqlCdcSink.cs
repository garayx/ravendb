using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Raven.Client.Documents.Operations.CDC;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;

namespace Raven.Server.Documents.CDC;

public sealed class PostgresqlCdcSink : CdcSinkProcess
{
    public PostgresqlCdcSink(CdcSinkConfiguration configuration, ulong lastLsn, CdcSinkScript script, DocumentDatabase database, string tag) : base(configuration, script, database, tag)
    {
        _testTables = configuration.Settings.Collections;

        LastLsn = new NpgsqlTypes.NpgsqlLogSequenceNumber(lastLsn);
    }

    public List<Collection2> _testTables { get; set; }

    public NpgsqlTypes.NpgsqlLogSequenceNumber LastLsn { get; set; }

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

                //TODO: egor pass from configuration 
                //{string.Join(", ", new List<string>(){ "Order" }/*Configuration.Connection.PostgresqlConnectionSettings.PostgresTableNames*/)}



                await using var createCmd = new NpgsqlCommand(
                    $"CREATE PUBLICATION {Configuration.Connection.PostgresqlConnectionSettings.PostgresPublicationName} FOR TABLE {_testTables};",
                    conn);
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
            }
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
        if (LastLsn > new NpgsqlTypes.NpgsqlLogSequenceNumber(0))
        {
            return;
        }

        try
        {
     //       await CleanupReplicationSlotsBySlotsNameAsync(CancellationToken);
            await CleanupAllReplicationSlotsAsync(CancellationToken);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        try
        {


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
            var dbDriver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
            var schema = dbDriver.FindSchema();
            // Now query the table. This query is "frozen" at the exact moment the slot was created.
            int existingRowCount = 0;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new SqlMigrationWriter(context, Configuration.Settings.BatchSize))
            {
                foreach (var table in _testTables)
                {



                    var tableSchema = schema.GetTable(table.SourceTableSchema, table.SourceTableName);

                    if (table.SourceTableName == "Order")
                    {

                    }
                    await using var selectCmd = new NpgsqlCommand(dbDriver.GetSelectAllQueryForTable($"'{table.SourceTableSchema}'", $"'{table.SourceTableName}'"), regularConn, tx);
          //      var q = $"SELECT * FROM \"{table.SourceTableName};\"";
                 //   await using var selectCmd = new NpgsqlCommand(q, regularConn, tx);
                    await using var reader = await selectCmd.ExecuteReaderAsync(CancellationToken);
                    var references = new List<ReferenceInformation>();
                    while (await reader.ReadAsync(CancellationToken))
                    {
                        // TODO: Map and save your historical documents here

                        var doc = new SqlMigrationDocument
                        {
                            Object = GenericDatabaseMigrator.ExtractFromReader(reader, table.ColumnsMapping),
                            Attachments = new Dictionary<string, byte[]>()
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
            Console.WriteLine($"Initial sync complete. Processed {existingRowCount} historical rows.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}

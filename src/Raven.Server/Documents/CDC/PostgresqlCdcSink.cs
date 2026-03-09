using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Raven.Client.Documents.Operations.CDC;

namespace Raven.Server.Documents.CDC;

public sealed class PostgresqlCdcSink : CdcSinkProcess
{
    public PostgresqlCdcSink(CdcSinkConfiguration configuration, ulong lastLsn, CdcSinkScript script, DocumentDatabase database, string tag) : base(configuration, script, database, tag)
    {

    
        LastLsn = new NpgsqlTypes.NpgsqlLogSequenceNumber(lastLsn);
    }

    string _testTables = "\"Order\", \"customer\", \"category\", \"orderitem\", \"details\", \"product\", \"photo\", \"productcategory\"";

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

    protected override async Task HandleInitialLoadAsync()
    {
        if (LastLsn > new NpgsqlTypes.NpgsqlLogSequenceNumber(0))
        {
            return;
        }

        try
        {
            var conn = new LogicalReplicationConnection(Configuration.Connection.PostgresqlConnectionSettings.ConnectionString);
        
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

            foreach (var table in _testTables.Split(", "))
            {
                await using var selectCmd = new NpgsqlCommand($"SELECT * FROM {table};", regularConn, tx);
                await using var reader = await selectCmd.ExecuteReaderAsync(CancellationToken);

                while (await reader.ReadAsync(CancellationToken))
                {
                    // TODO: Map and save your historical documents here
                    existingRowCount++;
                }
                await reader.CloseAsync();

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

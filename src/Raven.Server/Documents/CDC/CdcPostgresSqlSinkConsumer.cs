using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Raven.Server.Documents.QueueSink;

namespace Raven.Server.Documents.CDC;

public class CdcPostgresSqlSinkConsumer : ICdcSinkConsumer
{
    private readonly LogicalReplicationConnection _conn;
    private IAsyncEnumerator<PgOutputReplicationMessage> _consumer;

    public CdcPostgresSqlSinkConsumer(LogicalReplicationConnection conn, IAsyncEnumerable<PgOutputReplicationMessage> replicationStream)
    {
        _conn = conn;
        _consumer = replicationStream.GetAsyncEnumerator();
    }

    public async Task<PgOutputReplicationMessage> ConsumeAsync(CancellationToken cancellationToken)
    {
        var hasMore = await _consumer.MoveNextAsync();

        if (hasMore == false)
            return null;

        var message = _consumer.Current;

        Debug.Assert(message != null, "message != null");



        return message;
    }

    public byte[] Consume(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    public void Commit(NpgsqlLogSequenceNumber lastLsn)
    {
        _conn.SetReplicationStatus(lastLsn);

    }

    public async ValueTask DisposeAsync()
    {
        if (_conn != null) await _conn.DisposeAsync();
        if (_consumer != null) await _consumer.DisposeAsync();
    }
}

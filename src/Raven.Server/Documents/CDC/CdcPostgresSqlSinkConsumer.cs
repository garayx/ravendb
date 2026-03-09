using System;
using System.Collections.Generic;
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
    private NpgsqlLogSequenceNumber _lastLsn;

    public CdcPostgresSqlSinkConsumer(LogicalReplicationConnection conn, IAsyncEnumerable<PgOutputReplicationMessage> replicationStream)
    {
        _conn = conn;
        _consumer = replicationStream.GetAsyncEnumerator();
    }

    public async Task<PgOutputReplicationMessage> ConsumeAsync(CancellationToken cancellationToken)
    {
        var vt = await _consumer.MoveNextAsync();

        var message = _consumer.Current;

        if (message is CommitMessage commit)
        {
            _lastLsn = commit.CommitLsn;
        }

        return message;
    }

    public byte[] Consume(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    public void Commit()
    {
        _conn.SetReplicationStatus(_lastLsn);
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn != null) await _conn.DisposeAsync();
        if (_consumer != null) await _consumer.DisposeAsync();
    }
}

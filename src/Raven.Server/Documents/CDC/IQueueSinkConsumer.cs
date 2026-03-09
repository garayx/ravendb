using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace Raven.Server.Documents.CDC;

public interface ICdcSinkConsumer : IAsyncDisposable
{
    public Task<PgOutputReplicationMessage> ConsumeAsync(CancellationToken cancellationToken);

    //public byte[] Consume(TimeSpan timeout);

    public void Commit();
}

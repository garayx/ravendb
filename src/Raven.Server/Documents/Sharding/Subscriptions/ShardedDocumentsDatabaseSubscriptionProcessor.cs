using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private ShardingConfiguration _sharding;
    private readonly ByteStringContext _allocator;

    public ShardedDocumentsDatabaseSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
        _database = database;
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
    }

    protected override SubscriptionFetcher<Document> CreateFetcher()
    {
        _sharding = _database.ShardingConfiguration;
        return base.CreateFetcher();
    }

    protected override ConflictStatus GetConflictStatus(Document item)
    {
        SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(_database.Name, out var cv);
        var conflictStatus = ChangeVectorUtils.GetConflictStatus(
            remoteAsString: item.ChangeVector,
            localAsString: cv);
        return conflictStatus;
    }

    protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result, out bool isActiveMigration)
    {
        exception = null;
        result = item;
        isActiveMigration = false;

        var bucket = ShardHelper.GetBucketFor(_sharding, _allocator, item.Id);
        var shard = ShardHelper.GetShardNumberFor(_sharding, bucket);
        if (_sharding.BucketMigrations.TryGetValue(bucket, out var migration) && migration.IsActive)
        {
            var s = ShardHelper.GetShardNumberFor(_sharding, bucket);
            reason = $"The document '{item.Id}' from bucket '{bucket}' is under active migration to shard '{s}' (current shard number: '{_database.ShardNumber}').";
            item.Data = null;
            item.ChangeVector = string.Empty;
            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage) //TODO: egor maybe here also mark as isActiveMigration on fetching from resend
            {
                isActiveMigration = true;
            }
            //TODO: egor add this
            /*if (shard == _database.ShardNumber)
            {
                isActiveMigration = true;
            }*/

            return false;
        }

        if (shard != _database.ShardNumber)
        {
            reason = $"The owner of '{item.Id}' document is shard '{shard}' (current shard number: '{_database.ShardNumber}').";
            item.Data = null;
            item.ChangeVector = string.Empty;
            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                isActiveMigration = true;
            }

            return false;
        }

        return base.ShouldSend(item, out reason, out exception, out result, out isActiveMigration);
    }

    public override void Dispose()
    {
        base.Dispose();

        _allocator?.Dispose();
    }

    protected override bool ShouldFetchFromResend(DocumentsOperationContext context, string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector, out string reason)
    {
        reason = null;
        if (item.Document == null)
        {
            // the document was delete while it was processed by the client
            ItemsToRemoveFromResend.Add(id);
            reason = $"document '{id}' removed and skipped from resend";
            return false;
        }

        var cv = context.GetChangeVector(item.Document.ChangeVector);
        if (cv.IsSingle)
            return base.ShouldFetchFromResend(context, id, item, currentChangeVector, out reason);

        item.Document.ChangeVector = context.GetChangeVector(cv.Version, cv.Order.RemoveId(_sharding.DatabaseId, context));

        return base.ShouldFetchFromResend(context, id, item, currentChangeVector, out reason);
    }

    public HashSet<string> Skipped;

    public override async Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
    {
        // command to all active batches of orchestrator

        var result = await SubscriptionConnectionsState.RecordBatchDocuments(BatchItems, ItemsToRemoveFromResend, lastChangeVectorSentInThisBatch);
        Skipped = result.Skipped as HashSet<string>;
        return result.Index;
    }

    public override async Task AcknowledgeBatch(long batchId, string changevector)
    {
        ItemsToRemoveFromResend.Clear();
        BatchItems.Clear();

        await SubscriptionConnectionsState.AcknowledgeShardingBatch(_connection.LastSentChangeVectorInThisConnection, changevector, batchId, BatchItems);
    }

    protected override ShardIncludesCommandImpl CreateIncludeCommands()
    {
        var hasIncludes = TryCreateIncludesCommand(Database, DocsContext, Connection, Connection.Subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries);
        var includes = hasIncludes ? new ShardIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters) : null;

        return includes;
    }
}

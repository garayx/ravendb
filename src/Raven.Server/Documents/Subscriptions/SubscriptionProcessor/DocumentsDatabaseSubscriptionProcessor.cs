﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class DocumentsDatabaseSubscriptionProcessor : DatabaseSubscriptionProcessor<Document>
    {
        protected readonly SubscriptionConnection _connection;

        public DocumentsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
            _connection = connection;
        }

        public override IEnumerable<(Document Doc, Exception Exception, bool IsActiveMigration)> GetBatch()
        {
            Size size = default;
            var numberOfDocs = 0;

            BatchItems.Clear();
            ItemsToRemoveFromResend.Clear();

            foreach (var item in Fetcher.GetEnumerator())
            {
                size += new Size(item.Data?.Size ?? 0, SizeUnit.Bytes);

                var result = GetBatchItem(item);

                if (result.Doc.Data != null)
                {
                    BatchItems.Add(new DocumentRecord
                    {
                        DocumentId = result.Doc.Id,
                        ChangeVector = result.Doc.ChangeVector,
                    });

                    yield return result;

                    if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                        yield break;

                    if (++numberOfDocs >= BatchSize)
                        yield break;
                }
                else
                {
                    item.Data?.Dispose();
                    item.Data = null;
                    yield return result;
                }
            }
        }

        public List<string> ItemsToRemoveFromResend = new List<string>();
        public List<DocumentRecord> BatchItems = new List<DocumentRecord>();

        public override async Task<long> RecordBatch(string lastChangeVectorSentInThisBatch) =>
            (await SubscriptionConnectionsState.RecordBatchDocuments(BatchItems, ItemsToRemoveFromResend, lastChangeVectorSentInThisBatch)).Index;

        public override async Task AcknowledgeBatch(long batchId, string changevector)
        {
            ItemsToRemoveFromResend.Clear();

            //pick up docs that weren't sent due to having been processed by this connection and add them to resend
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docContext))
            using (docContext.OpenReadTransaction())
            {
                for (var index = BatchItems.Count - 1; index >= 0; index--)
                {
                    var doc = BatchItems[index];
                    var document = Database.DocumentsStorage.GetDocumentOrTombstone(docContext, doc.DocumentId, throwOnConflict: false);
                    if (ShouldAddToResendTable(docContext, document, doc.ChangeVector) == false)
                    {
                        BatchItems.RemoveAt(index);
                    }
                }
            }

            await SubscriptionConnectionsState.AcknowledgeBatch(_connection.LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange), batchId, BatchItems);

            if (BatchItems?.Count > 0)
            {
                SubscriptionConnectionsState.NotifyHasMoreDocs();
                BatchItems.Clear();
            }
        }

        public override long GetLastItemEtag(DocumentsOperationContext context, string collection)
        {
            var isAllDocs = collection == Constants.Documents.Collections.AllDocumentsCollection;

            if (isAllDocs)
                return DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
        }


        protected override SubscriptionFetcher<Document> CreateFetcher()
        {
            return new DocumentSubscriptionFetcher(Database, SubscriptionConnectionsState, Collection);
        }

        protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result, out bool isActiveMigration)
        {
            exception = null;
            reason = null;
            result = item;
            isActiveMigration = false;

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                var conflictStatus = GetConflictStatus(item);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{item.Id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsDocumentInActiveBatch(ClusterContext, item.Id, Active))
                {
                    reason = $"{item.Id} exists in an active batch";
                    return false;
                }
            }

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
            {
                var current = Database.DocumentsStorage.GetDocumentOrTombstone(DocsContext, item.Id, throwOnConflict: false);
                if (ShouldFetchFromResend(DocsContext, item.Id, current, item.ChangeVector, out reason) == false)
                {
                    item.ChangeVector = string.Empty;
                    return false;
                }

                Debug.Assert(current.Document != null, "Document does not exist");
                result.Id = current.Document.Id; // use proper casing
                result.Data = current.Document.Data;
                result.ChangeVector = current.Document.ChangeVector;
            }

            if (Patch == null)
                return true;

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, item, ProjectionMetadataModifier.Instance, ref result.Data);

                if (match == false)
                {
                    if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
                    {
                        item.ChangeVector = string.Empty;
                        ItemsToRemoveFromResend.Add(item.Id);
                    }

                    result.Data = null;
                    reason = $"{item.Id} filtered out by criteria";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                exception = ex;
                reason = $"Criteria script threw exception for document id {item.Id}";
                return false;
            }
        }

        protected virtual ConflictStatus GetConflictStatus(Document item)
        {
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: item.ChangeVector,
                localAsString: SubscriptionState.ChangeVectorForNextBatchStartingPoint);
            return conflictStatus;
        }

        protected virtual bool ShouldFetchFromResend(DocumentsOperationContext context, string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector, out string reason)
        {
            reason = null;
            if (item.Document == null)
            {
                // the document was delete while it was processed by the client
                ItemsToRemoveFromResend.Add(id);
                reason = $"document '{id}' removed and skipped from resend";
                return false;
            }
            // local: A:5-db1
            // remote B:10-db2|A:3-db1
            var status = Database.DocumentsStorage.GetConflictStatus(context, item.Document.ChangeVector, currentChangeVector, ChangeVectorMode.Version);
            switch (status)
            {
                case ConflictStatus.Update:
                    // If document was updated, but the subscription went too far.
                    var resendStatus = Database.DocumentsStorage.GetConflictStatus(context, item.Document.ChangeVector, SubscriptionConnectionsState.LastChangeVectorSent, ChangeVectorMode.Order);
                    if (resendStatus == ConflictStatus.Update)
                    {
                        // we can clear it from resend list, and it will processed as regular document
                        ItemsToRemoveFromResend.Add(id);
                        reason = $"document '{id}' was updated ({item.Document.ChangeVector}), but the subscription went too far and skipped from resend (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";
                        return false;
                    }

                    // We need to resend it
                    var fetch = resendStatus == ConflictStatus.AlreadyMerged;
                    if (fetch == false)
                        reason = $"document '{id}' is in status {resendStatus} (local: {item.Document.ChangeVector}) with the subscription progress (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";

                    return fetch;

                case ConflictStatus.AlreadyMerged:
                    if (currentChangeVector != item.Document.ChangeVector)
                    {
                        Console.WriteLine($"local: {currentChangeVector} != remote: {item.Document.ChangeVector}");
                    }
                    return true;

                case ConflictStatus.Conflict:
                    reason = $"document '{id}' is in conflict, CV in storage '{item.Document.ChangeVector}' CV in resend list '{currentChangeVector}' (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";
                    return false;

                default:
                    throw new ArgumentOutOfRangeException(nameof(ConflictStatus), status.ToString());
            }
        }

        private bool ShouldAddToResendTable(DocumentsOperationContext context, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                var status = Database.DocumentsStorage.GetConflictStatus(context, item.Document.ChangeVector, currentChangeVector, ChangeVectorMode.Version);
                switch (status)
                {
                    case ConflictStatus.Update:
                        return true;

                    case ConflictStatus.AlreadyMerged:
                    case ConflictStatus.Conflict:
                        return false;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(ConflictStatus), status.ToString());
                }
            }

            return false;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Server.Documents.CDC.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CDC.Commands;

public sealed class BatchCdcSinkScriptCommand : DocumentMergedTransactionCommand
{
    private readonly List<CdcSinkProcess.CdcChangeItem> _messages;
    private readonly string _script = string.Empty;
    private readonly CdcSinkStatsScope _scriptProcessingScope;
    private readonly CdcSinkProcessStatistics _statistics;
    private readonly RavenLogger _logger;

    public BatchCdcSinkScriptCommand(string script, List<CdcSinkProcess.CdcChangeItem> messages, CdcSinkStatsScope scriptProcessingScope,
        CdcSinkProcessStatistics statistics, RavenLogger logger)
    {
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _scriptProcessingScope = scriptProcessingScope ?? throw new ArgumentException($"{nameof(CdcSinkStatsScope)} cannot be null", nameof(scriptProcessingScope));
        _statistics = statistics ?? throw new ArgumentException($"{nameof(CdcSinkProcessStatistics)} cannot be null", nameof(statistics));
        _logger = logger ?? throw new ArgumentException($"{nameof(RavenLogger)} cannot be null", nameof(logger));
    }

    private BatchCdcSinkScriptCommand(List<CdcSinkProcess.CdcChangeItem> messages)
    {
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _scriptProcessingScope = null;
        _statistics = null;
        _logger = null;
    }

    // todo: egor this is same as above?
    internal BatchCdcSinkScriptCommand(List<CdcSinkProcess.CdcChangeItem> messages, bool initialLoad)
    {
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _scriptProcessingScope = null;
        _statistics = null;
        _logger = null;
    }

    public int ProcessedSuccessfully { get; private set; }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var processed = 0L;

        foreach (var item in _messages)
        {
            try
            {
                processed++;

                switch (item.ChangeType)
                {
                    case CdcSinkProcess.CdcChangeType.Put:
                        using (item.Document)
                        {
                            context.DocumentDatabase.DocumentsStorage.Put(context, item.Id, null, item.Document);
                        }
                        break;

                    case CdcSinkProcess.CdcChangeType.Delete:
                        context.DocumentDatabase.DocumentsStorage.Delete(context, item.Id, null);
                        break;

                    case CdcSinkProcess.CdcChangeType.NestedPut:
                        ProcessNestedPut(context, item);
                        break;

                    case CdcSinkProcess.CdcChangeType.NestedDelete:
                        ProcessNestedDelete(context, item);
                        break;
                }

                _scriptProcessingScope?.RecordProcessedMessage();
                ProcessedSuccessfully++;
            }
            catch (Exception e)
            {
                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process CDC change (type: {item.ChangeType}, id: {item.Id ?? item.ParentDocumentId}).", e);

                _scriptProcessingScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        return processed;
    }

    /// <summary>
    /// Handles a nested insert or update: loads the parent document, finds or adds
    /// the nested item in the array property, and saves the parent back.
    /// </summary>
    private void ProcessNestedPut(DocumentsOperationContext context, CdcSinkProcess.CdcChangeItem item)
    {
        var existingDoc = context.DocumentDatabase.DocumentsStorage.Get(context, item.ParentDocumentId);
        if (existingDoc == null)
        {
            if (_logger?.IsInfoEnabled == true)
                _logger.Info($"Parent document '{item.ParentDocumentId}' not found for nested put on '{item.NestedPropertyName}'. Skipping.");
            return;
        }

        var modifications = new DynamicJsonValue(existingDoc.Data);

        // Build the new nested array: keep existing items that don't match the PK, add/replace with the new item
        var newArray = new DynamicJsonArray();

        if (existingDoc.Data.TryGet(item.NestedPropertyName, out BlittableJsonReaderArray existingArray) && existingArray != null)
        {
            foreach (var existingItem in existingArray)
            {
                if (existingItem is BlittableJsonReaderObject existingObj && NestedItemMatchesKey(existingObj, item.NestedItemKey))
                {
                    // Skip the old version — we'll add the updated one below
                    continue;
                }
                newArray.Add(existingItem);
            }
        }

        // Add the new/updated nested item
        using (item.Document)
        {
            newArray.Add(item.Document.Clone(context));
        }

        modifications[item.NestedPropertyName] = newArray;

        using var updatedBlittable = context.ReadObject(modifications, item.ParentDocumentId);
        context.DocumentDatabase.DocumentsStorage.Put(context, item.ParentDocumentId, null, updatedBlittable);
    }

    /// <summary>
    /// Handles a nested delete: loads the parent document, removes the matching item
    /// from the nested array, and saves the parent back.
    /// </summary>
    private void ProcessNestedDelete(DocumentsOperationContext context, CdcSinkProcess.CdcChangeItem item)
    {
        var existingDoc = context.DocumentDatabase.DocumentsStorage.Get(context, item.ParentDocumentId);
        if (existingDoc == null)
        {
            if (_logger?.IsInfoEnabled == true)
                _logger.Info($"Parent document '{item.ParentDocumentId}' not found for nested delete on '{item.NestedPropertyName}'. Skipping.");
            return;
        }

        if (existingDoc.Data.TryGet(item.NestedPropertyName, out BlittableJsonReaderArray existingArray) == false ||
            existingArray == null || existingArray.Length == 0)
        {
            return; // nothing to delete
        }

        var modifications = new DynamicJsonValue(existingDoc.Data);
        var newArray = new DynamicJsonArray();
        bool found = false;

        foreach (var existingItem in existingArray)
        {
            if (existingItem is BlittableJsonReaderObject existingObj && NestedItemMatchesKey(existingObj, item.NestedItemKey))
            {
                found = true;
                continue; // skip the item to delete
            }
            newArray.Add(existingItem);
        }

        if (found == false)
            return; // item wasn't in the array

        modifications[item.NestedPropertyName] = newArray;

        using var updatedBlittable = context.ReadObject(modifications, item.ParentDocumentId);
        context.DocumentDatabase.DocumentsStorage.Put(context, item.ParentDocumentId, null, updatedBlittable);
    }

    /// <summary>
    /// Checks if a nested blittable object matches the given PK key values.
    /// The key dictionary maps lowercase PG column names to their values.
    /// The blittable properties are in PascalCase.
    /// </summary>
    private static bool NestedItemMatchesKey(BlittableJsonReaderObject obj, Dictionary<string, object> key)
    {
        foreach (var kvp in key)
        {
            // Properties in the nested document are stored in PascalCase
            var propName = char.ToUpper(kvp.Key[0]) + kvp.Key.Substring(1);

            if (obj.TryGetMember(propName, out var existingValue) == false)
                return false;

            if (existingValue == null && kvp.Value == null)
                continue;

            if (existingValue == null || kvp.Value == null)
                return false;

            // Compare as strings since blittable stores numbers as LazyStringValue sometimes
            if (string.Equals(existingValue.ToString(), kvp.Value.ToString(), StringComparison.Ordinal) == false)
                return false;
        }
        return true;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        var dto = new Dto
        {
            Script = _script,
            Messages = _messages.ToList()
        };

        return dto;
    }

    public class Dto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand>
    {
        public string Script { get; set; }

        public List<CdcSinkProcess.CdcChangeItem> Messages { get; set; }

        public DocumentMergedTransactionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BatchCdcSinkScriptCommand(Messages);
        }
    }
}

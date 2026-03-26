using System;
using System.Collections.Generic;
using System.Linq;
using NuGet.Protocol;
using Raven.Client;
using Raven.Server.Documents.CDC.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.CDC.Commands;

public sealed class BatchCdcSinkScriptCommand : DocumentMergedTransactionCommand
{
    private readonly List<CdcSinkProcess.CdcChangeItem> _messages;
    private readonly PostgresqlCdcSink.Config _config;
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
    // todo: egor this is same as above?
    internal BatchCdcSinkScriptCommand(List<CdcSinkProcess.CdcChangeItem> messages, PostgresqlCdcSink.Config config, bool initialLoad)
    {
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _config = config;
        _scriptProcessingScope = null;
        _statistics = null;
        _logger = null;
    }

    public int ProcessedSuccessfully { get; private set; }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var processed = 0L;

        // Group nested puts by parent document so we can apply all changes at once,
        // avoiding the issue where multiple Puts to the same document within a single
        // transaction are not visible to subsequent Gets.
        var nestedPutsByParent = new Dictionary<string, List<CdcSinkProcess.CdcChangeItem>>(StringComparer.OrdinalIgnoreCase);
        var nestedDeletesByParent = new Dictionary<string, List<CdcSinkProcess.CdcChangeItem>>(StringComparer.OrdinalIgnoreCase);

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
                        if (nestedPutsByParent.TryGetValue(item.ParentDocumentId, out var putList) == false)
                        {
                            putList = new List<CdcSinkProcess.CdcChangeItem>();
                            nestedPutsByParent[item.ParentDocumentId] = putList;
                        }
                        putList.Add(item);
                        break;

                    case CdcSinkProcess.CdcChangeType.NestedDelete:
                        if (nestedDeletesByParent.TryGetValue(item.ParentDocumentId, out var delList) == false)
                        {
                            delList = new List<CdcSinkProcess.CdcChangeItem>();
                            nestedDeletesByParent[item.ParentDocumentId] = delList;
                        }
                        delList.Add(item);
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

        // Apply all nested deletes grouped by parent
        foreach (var kvp in nestedDeletesByParent)
        {
            try
            {
                ProcessGroupedNestedDeletes(context, kvp.Key, kvp.Value);
            }
            catch (Exception e)
            {
                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process grouped nested deletes for parent '{kvp.Key}'.", e);

                _scriptProcessingScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        // Apply all nested puts grouped by parent
        foreach (var kvp in nestedPutsByParent)
        {
            try
            {
                ProcessGroupedNestedPuts(context, kvp.Key, kvp.Value);
            }
            catch (Exception e)
            {
                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process grouped nested puts for parent '{kvp.Key}'.", e);

                _scriptProcessingScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        if (_config == null)
        {
            //TODO: egor handle that case in logical replication
                        return processed;
        }

        using (var freshHilo = context.ReadObject(_config.ToJson(), _config.CdcConfigId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            context.DocumentDatabase.DocumentsStorage.Put(context, _config.CdcConfigId, null, freshHilo, nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);


        return processed;
    }

    /// <summary>
    /// Applies all nested puts for a single parent document in one Get-Modify-Put cycle.
    /// This avoids the problem where multiple Puts within the same write transaction
    /// are not visible to subsequent Gets.
    /// </summary>
    private void ProcessGroupedNestedPuts(DocumentsOperationContext context, string parentDocumentId, List<CdcSinkProcess.CdcChangeItem> items)
    {
        var existingDoc = context.DocumentDatabase.DocumentsStorage.Get(context, parentDocumentId);
        if (existingDoc == null)
        {
            if (_logger?.IsInfoEnabled == true)
                _logger.Info($"Parent document '{parentDocumentId}' not found for nested put. Skipping.");
            return;
        }

        var modifications = new DynamicJsonValue(existingDoc.Data);

        // Group items by nested property name since a parent could have multiple nested collections
        var itemsByProperty = new Dictionary<string, List<CdcSinkProcess.CdcChangeItem>>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in items)
        {
            if (itemsByProperty.TryGetValue(item.NestedPropertyName, out var list) == false)
            {
                list = new List<CdcSinkProcess.CdcChangeItem>();
                itemsByProperty[item.NestedPropertyName] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in itemsByProperty)
        {
            var propertyName = kvp.Key;
            var propertyItems = kvp.Value;

            var newArray = new DynamicJsonArray();

            if (existingDoc.Data.TryGet(propertyName, out BlittableJsonReaderArray existingArray) && existingArray != null)
            {
                foreach (var existingItem in existingArray)
                {
                    bool shouldSkip = false;
                    if (existingItem is BlittableJsonReaderObject existingObj)
                    {
                        foreach (var newItem in propertyItems)
                        {
                            if (NestedItemMatchesKey(existingObj, newItem.NestedItemKey))
                            {
                                shouldSkip = true;
                                break;
                            }
                        }
                    }
                    if (shouldSkip == false)
                        newArray.Add(existingItem);
                }
            }

            foreach (var item in propertyItems)
            {
                using (item.Document)
                {
                    newArray.Add(item.Document.Clone(context));
                }
            }

            modifications[propertyName] = newArray;
        }

        existingDoc.Data.Modifications = modifications;
        using var updatedBlittable = context.ReadObject(existingDoc.Data, parentDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        context.DocumentDatabase.DocumentsStorage.Put(context, parentDocumentId, null, updatedBlittable);
    }

    /// <summary>
    /// Applies all nested deletes for a single parent document in one Get-Modify-Put cycle.
    /// </summary>
    private void ProcessGroupedNestedDeletes(DocumentsOperationContext context, string parentDocumentId, List<CdcSinkProcess.CdcChangeItem> items)
    {
        var existingDoc = context.DocumentDatabase.DocumentsStorage.Get(context, parentDocumentId);
        if (existingDoc == null)
        {
            if (_logger?.IsInfoEnabled == true)
                _logger.Info($"Parent document '{parentDocumentId}' not found for nested delete. Skipping.");
            return;
        }

        // Group items by nested property name
        var itemsByProperty = new Dictionary<string, List<CdcSinkProcess.CdcChangeItem>>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in items)
        {
            if (itemsByProperty.TryGetValue(item.NestedPropertyName, out var list) == false)
            {
                list = new List<CdcSinkProcess.CdcChangeItem>();
                itemsByProperty[item.NestedPropertyName] = list;
            }
            list.Add(item);
        }

        var modifications = new DynamicJsonValue(existingDoc.Data);
        bool anyChanges = false;

        foreach (var kvp in itemsByProperty)
        {
            var propertyName = kvp.Key;
            var propertyItems = kvp.Value;

            if (existingDoc.Data.TryGet(propertyName, out BlittableJsonReaderArray existingArray) == false ||
                existingArray == null || existingArray.Length == 0)
                continue;

            var newArray = new DynamicJsonArray();
            bool foundAny = false;

            foreach (var existingItem in existingArray)
            {
                bool shouldDelete = false;
                if (existingItem is BlittableJsonReaderObject existingObj)
                {
                    foreach (var deleteItem in propertyItems)
                    {
                        if (NestedItemMatchesKey(existingObj, deleteItem.NestedItemKey))
                        {
                            shouldDelete = true;
                            foundAny = true;
                            break;
                        }
                    }
                }
                if (shouldDelete == false)
                    newArray.Add(existingItem);
            }

            if (foundAny)
            {
                modifications[propertyName] = newArray;
                anyChanges = true;
            }
        }

        if (anyChanges)
        {
            existingDoc.Data.Modifications = modifications;
            using var updatedBlittable = context.ReadObject(existingDoc.Data, parentDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            context.DocumentDatabase.DocumentsStorage.Put(context, parentDocumentId, null, updatedBlittable);
        }
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

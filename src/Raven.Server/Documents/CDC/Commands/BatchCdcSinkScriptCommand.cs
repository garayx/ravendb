using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.CDC.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
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
                }

                _scriptProcessingScope?.RecordProcessedMessage();
                ProcessedSuccessfully++;
            }
            catch (Exception e)
            {
                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process CDC change (type: {item.ChangeType}, id: {item.Id}).", e);

                _scriptProcessingScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        return processed;
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

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
    private readonly List<(string, BlittableJsonReaderObject)> _messages;
    private readonly string _script = string.Empty;
    private readonly CdcSinkStatsScope _scriptProcessingScope;
    private readonly CdcSinkProcessStatistics _statistics;
    private readonly RavenLogger _logger;

    public BatchCdcSinkScriptCommand(string script, List<(string, BlittableJsonReaderObject)> messages, CdcSinkStatsScope scriptProcessingScope,
        CdcSinkProcessStatistics statistics, RavenLogger logger)
    {
        //TODO: egor pass the Patch per collection here
        //_script = script ?? throw new ArgumentException("Script cannot be null", nameof(script));
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _scriptProcessingScope = scriptProcessingScope ?? throw new ArgumentException($"{nameof(CdcSinkStatsScope)} cannot be null", nameof(scriptProcessingScope));
        _statistics = statistics ?? throw new ArgumentException($"{nameof(CdcSinkProcessStatistics)} cannot be null", nameof(statistics));
        _logger = logger ?? throw new ArgumentException($"{nameof(RavenLogger)} cannot be null", nameof(logger));
    }

    private BatchCdcSinkScriptCommand(string script, List<(string, BlittableJsonReaderObject)> messages)
    {
        //TODO: egor pass the Patch per collection here
        //_script = script ?? throw new ArgumentException("Script cannot be null", nameof(script));
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _scriptProcessingScope = null;
        _statistics = null;
        _logger = null;
    }

    public int ProcessedSuccessfully { get; private set; }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        try
        {
            var mainScript = new PatchRequest(_script, PatchRequestType.CdcSink);

            using (context.DocumentDatabase.Scripts.GetScriptRunner(mainScript, readOnly: false, out var documentScript))
            {
                var processed = 0L;

                foreach (var tuple in _messages)
                {
                    try
                    {
                        processed++;
                 var       message = tuple.Item2;
                 var id = tuple.Item1;
                        using (message)
                        using (documentScript.Run(context, context, "execute", new object[] {message}))
                        {
                            //TODO: egor write to db :)
                            Console.WriteLine(message);
                            //message.TryGet("id", out string id);
                            context.DocumentDatabase.DocumentsStorage.Put(context, id, null, message);
                        }

                        _scriptProcessingScope?.RecordProcessedMessage();
                        ProcessedSuccessfully++;
                    }
                    catch (Exception e)
                    {
                        if (_logger?.IsErrorEnabled == true)
                            _logger.Error("Failed to process consumed message by the script.", e);

                        _scriptProcessingScope?.RecordScriptProcessingError();
                        _statistics?.RecordScriptExecutionError(e);
                    }
                }

                return processed;
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
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

        public List<(string, BlittableJsonReaderObject)> Messages { get; set; }

        public DocumentMergedTransactionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BatchCdcSinkScriptCommand(Script, Messages);
        }
    }
}

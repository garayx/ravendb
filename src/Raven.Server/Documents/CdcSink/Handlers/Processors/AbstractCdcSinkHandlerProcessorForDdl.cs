using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal abstract class AbstractCdcSinkHandlerProcessorForDdl<TRequestHandler, TOperationContext>
    : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractCdcSinkHandlerProcessorForDdl([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }
}

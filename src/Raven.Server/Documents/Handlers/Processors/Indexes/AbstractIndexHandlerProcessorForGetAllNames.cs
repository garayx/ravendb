﻿using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetAllNames<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<string[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForGetAllNames([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name", required: false);
    }

    protected override RavenCommand<string[]> CreateCommandForNode(string nodeTag) => new GetIndexNamesOperation.GetIndexNamesCommand(RequestHandler.GetStart(), RequestHandler.GetPageSize(), nodeTag);
}

﻿using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexProcessorForGenerateCSharpIndexDefinition<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<string, TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexProcessorForGenerateCSharpIndexDefinition([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand<string> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();

        return new GenerateCSharpIndexDefinitionCommand(name, nodeTag);
    }

    protected override async ValueTask WriteResultAsync(string result)
    {
        await using (var writer = new StreamWriter(RequestHandler.ResponseBodyStream()))
        {
            await writer.WriteAsync(result);
        }
    }

    protected string GetName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
}

using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.CdcSink.Schema.Ddl;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForDdl : AbstractCdcSinkHandlerProcessorForDdl<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CdcSinkHandlerProcessorForDdl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(RequestHandler.Database.DatabaseShutdown, HttpContext.RequestAborted))
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "CdcSinkDdlRequest");
            var request = JsonDeserializationClient.CdcSinkDdlRequest(bodyJson);

            Validate(request);

            SqlConnectionString connection;
            CdcSinkDdlExporter exporter;
            try
            {
                connection = CdcSinkRequestValidation.ResolveSqlConnection(
                    RequestHandler.Database,
                    request.Connection,
                    request.ConnectionStringName,
                    inlineFieldName: nameof(CdcSinkDdlRequest.Connection),
                    namedFieldName: nameof(CdcSinkDdlRequest.ConnectionStringName));
                exporter = CdcSinkDdlExporter.For(connection.FactoryName);
            }
            catch (InvalidOperationException e)
            {
                throw new BadRequestException(e.Message, e);
            }

            var export = await exporter.ExportAsync(connection.ConnectionString, request.Schemas, request.Tables, cts.Token);
            if (export.Files.Count == 0)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            await using (var ms = new MemoryStream())
            {
                using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
                {
                    foreach (var file in export.Files)
                    {
                        var entry = archive.CreateEntry(file.FileName);
                        await using (var entryStream = entry.Open())
                        await using (var writer = new StreamWriter(entryStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)))
                            await writer.WriteAsync(file.Sql);
                    }
                }

                HttpContext.Response.ContentType = "application/zip";
                var fileName = Uri.EscapeDataString((export.CatalogName ?? "schema") + "-ddl.zip");
                HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";

                ms.Position = 0;
                await ms.CopyToAsync(RequestHandler.ResponseBodyStream(), cts.Token);
            }
        }
    }

    private static void Validate(CdcSinkDdlRequest request)
    {
        if (request.Schemas != null)
        {
            foreach (var schemaName in request.Schemas)
            {
                if (string.IsNullOrEmpty(schemaName))
                    throw new BadRequestException($"'{nameof(CdcSinkDdlRequest.Schemas)}' must not contain empty entries.");
                if (CdcSinkRequestValidation.TryValidateIdentifier(schemaName, $"{nameof(CdcSinkDdlRequest.Schemas)}[]", allowEmpty: false, out var error) == false)
                    throw new BadRequestException(error);
            }
        }

        if (request.Tables != null)
        {
            foreach (var tableName in request.Tables)
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new BadRequestException($"'{nameof(CdcSinkDdlRequest.Tables)}' must not contain empty entries.");
            }
        }
    }
}

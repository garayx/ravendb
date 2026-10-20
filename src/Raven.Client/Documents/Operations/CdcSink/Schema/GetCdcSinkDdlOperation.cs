using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

internal class GetCdcSinkDdlOperation : IMaintenanceOperation<CdcSinkDdlResult>
{
    private readonly CdcSinkDdlRequest _request;

    public GetCdcSinkDdlOperation(SqlConnectionString connection, string[] schemas = null, string[] tables = null)
        : this(new CdcSinkDdlRequest { Connection = connection ?? throw new ArgumentNullException(nameof(connection)), Schemas = schemas, Tables = tables })
    {
    }

    public GetCdcSinkDdlOperation(string connectionStringName, string[] schemas = null, string[] tables = null)
        : this(new CdcSinkDdlRequest { ConnectionStringName = connectionStringName ?? throw new ArgumentNullException(nameof(connectionStringName)), Schemas = schemas, Tables = tables })
    {
    }

    public GetCdcSinkDdlOperation(CdcSinkDdlRequest request)
    {
        _request = request ?? throw new ArgumentNullException(nameof(request));
    }

    public RavenCommand<CdcSinkDdlResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new GetCdcSinkDdlCommand(conventions, _request);
    }

    private sealed class GetCdcSinkDdlCommand : RavenCommand<CdcSinkDdlResult>
    {
        private readonly CdcSinkDdlRequest _request;
        private readonly DocumentConventions _conventions;

        public GetCdcSinkDdlCommand(DocumentConventions conventions, CdcSinkDdlRequest request)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _request = request ?? throw new ArgumentNullException(nameof(request));
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink/ddl";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, ctx.ReadObject(_request.ToJson(), "CdcSinkDdlRequest")).ConfigureAwait(false),
                    _conventions),
            };
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            if (response == null || stream == null)
                return;

            using (var ms = new MemoryStream())
            {
                stream.CopyTo(ms);
                Result = new CdcSinkDdlResult { ZipContent = ms.ToArray() };
            }
        }
    }
}

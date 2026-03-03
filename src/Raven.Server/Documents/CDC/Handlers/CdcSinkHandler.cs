using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.CDC.Handlers;

public class CdcSinkHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/cdc-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        //using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        //{
        //    var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestQueueSinkScript");
        //    var testScript = JsonDeserializationServer.TestQueueSinkScript(dbDoc);

        //    var result = QueueSinkProcess.TestScript(testScript, context, Database);

        //    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        //    {
        //        context.Write(writer, result.ToJson());
        //    }
        //}
    }

    [RavenAction("/databases/*/cdc-sink/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
    public async Task PerformanceLive()
    {
        //using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        //{
        //    var sinks = GetProcessesToReportOn();

        //    var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        //    var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

        //    await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        //    using (var collector = new LiveQueueSinkPerformanceCollector(Database, sinks))
        //    {
        //        // 1. Send data to webSocket without making UI wait upon opening webSocket
        //        await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

        //        // 2. Send data to webSocket when available
        //        while (Database.DatabaseShutdown.IsCancellationRequested == false)
        //        {
        //            if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
        //            {
        //                break;
        //            }
        //        }
        //    }
        //}
    }

    //private Dictionary<string, List<QueueSinkProcess>> GetProcessesToReportOn()
    //{
    //    Dictionary<string, List<QueueSinkProcess>> sinks;
    //    var names = HttpContext.Request.Query["name"];

    //    if (names.Count == 0)
    //        sinks = Database.QueueSinkLoader.Processes
    //            .GroupBy(x => x.Configuration.Name)
    //            .OrderBy(x => x.Key)
    //            .ToDictionary(x => x.Key, x => x.OrderBy(y => y.Script.Name).ToList());
    //    else
    //    {
    //        sinks = Database.QueueSinkLoader.Processes
    //            .Where(x => names.Contains(x.Configuration.Name, StringComparer.OrdinalIgnoreCase) || names.Contains(x.Name, StringComparer.OrdinalIgnoreCase))
    //            .GroupBy(x => x.Configuration.Name)
    //            .OrderBy(x => x.Key)
    //            .ToDictionary(x => x.Key, x => x.OrderBy(y => y.Script.Name).ToList());
    //    }

    //    return sinks;
    //}
}

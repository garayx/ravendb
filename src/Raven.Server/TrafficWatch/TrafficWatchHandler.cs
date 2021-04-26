// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchHandler : RequestHandler
    {
        [RavenAction("/admin/traffic-watch", "GET", AuthorizationStatus.Operator)]
        public async Task TrafficWatchWebsockets()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var logger = ServerStore.Logger.GetLoggerFor(nameof(TrafficWatchHandler), LogType.Server);
                    try
                    {
                        var resourceName = GetStringQueryString("resourceName", required: false);
                        resourceName = resourceName != null ? "db/" + resourceName : null;
                        var connection = new TrafficWatchConnection(webSocket, resourceName, context, logger, ServerStore.ServerShutdown);
                        TrafficWatchManager.AddConnection(connection);
                        await connection.StartSendingNotifications();
                    }
                    catch (IOException)
                    {
                        // nothing to do - connection closed
                    }
                    catch (Exception ex)
                    {
                        if (logger.IsInfoEnabled)
                            logger.Info("Error encountered in TrafficWatch handler", ex);

                        try
                        {
                            await using (var ms = new MemoryStream())
                            {
                                await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Exception"] = ex
                                    });
                                }

                                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (logger.IsInfoEnabled)
                                logger.Info("Failed to send the error in TrafficWatch handler to the client", ex);
                        }
                    }
                }
            }
        }
    }
}

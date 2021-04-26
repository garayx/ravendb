using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Primitives;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminLogsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/logs/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(GetLogsConfigurationResult.CurrentMode)] = LoggingSource.Instance.LogMode,
                    [nameof(GetLogsConfigurationResult.Mode)] = ServerStore.Configuration.Logs.Mode,
                    [nameof(GetLogsConfigurationResult.Path)] = ServerStore.Configuration.Logs.Path.FullPath,
                    [nameof(GetLogsConfigurationResult.UseUtcTime)] = ServerStore.Configuration.Logs.UseUtcTime,
                    [nameof(GetLogsConfigurationResult.RetentionTime)] = LoggingSource.Instance.RetentionTime,
                    [nameof(GetLogsConfigurationResult.RetentionSize)] = LoggingSource.Instance.RetentionSize == long.MaxValue ? null : (object)LoggingSource.Instance.RetentionSize,
                    [nameof(GetLogsConfigurationResult.Compress)] = LoggingSource.Instance.Compressing
                };

                var json = context.ReadObject(djv, "logs/configuration");

                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "logs/configuration");

                var configuration = JsonDeserializationServer.Parameters.SetLogsConfigurationParameters(json);

                if (configuration.RetentionTime == null)
                    configuration.RetentionTime = ServerStore.Configuration.Logs.RetentionTime?.AsTimeSpan;

                LoggingSource.Instance.SetupLogMode(
                    configuration.Mode,
                    Server.Configuration.Logs.Path.FullPath,
                    configuration.RetentionTime,
                    configuration.RetentionSize?.GetValue(SizeUnit.Bytes),
                    configuration.Compress);
            }

            NoContentStatus();
        }

        [RavenAction("/admin/logs/watch", "GET", AuthorizationStatus.Operator)]
        public async Task RegisterForLogs()
        {
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var context = new LoggingSource.WebSocketContext();

                foreach (var filter in HttpContext.Request.Query["only"])
                {
                    context.Filter.Add(filter, true);
                }
                foreach (var filter in HttpContext.Request.Query["except"])
                {
                    context.Filter.Add(filter, false);
                }

                await LoggingSource.Instance.Register(socket, context, ServerStore.ServerShutdown);
            }
        }

        //  var djv = new DynamicJsonValue();
        //      var dja = new DynamicJsonArray();

        //       djv["InstanceLoggers"] = NewMethod(LoggingSource.Instance.Loggers);
        //       djv["ServerLoggers"] = NewMethod(ServerStore.serverStoreLogger.Loggers);


        //var djv = new DynamicJsonValue
        //{
        //    ["ServerLoggers"] = NewMethod(ServerStore.serverStoreLogger.Loggers),
        //};
        //if (ServerStore.serverStoreLogger.Loggers.TryGetValue(nameof(DocumentDatabase), out var dbLoggers))
        //{
        //    djv["DatabaseLoggers"] = NewMethod(dbLoggers.Logger.Loggers);
        //}

        [RavenAction("/admin/loggers", "GET", AuthorizationStatus.Operator)]
        public Task GetAllLoggers()
        {
            var typeString = GetStringQueryString("type", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                DynamicJsonValue djv = null;
                if (string.IsNullOrEmpty(typeString) == false)
                {
                    var type = (LogType)Enum.Parse(typeof(LogType), typeString);

                    switch (type)
                    { 
                        case LogType.Server:
                            djv = new DynamicJsonValue
                            {
                                //TODO: call all Loggers
                                ["ServerLoggers"] = GetAllSubLoggers(Server.RavenServerLogger.Loggers)
                            };
                            break;
                        case LogType.Cluster:
                            var count = 0;
                            var dja = new DynamicJsonArray();
                            var dic = new Dictionary<string, LoggingSourceHolder>();
                            foreach (var holder in ServerStore.Logger.Loggers)
                            {
                                if ((LogType)Enum.Parse(typeof(LogType), holder.Value.Type) == LogType.Cluster)
                                {
                                    dic.Add(holder.Value.Source, holder.Value);
                                }
                            }
                            if (dic.Count > 0)
                            {
                                djv = new DynamicJsonValue
                                {
                             //       ["Count"] = count,
                                    ["ClusterLoggers"] = GetAllSubLoggers(dic),
                                };
                            }

                            break;
                        case LogType.Client:
                            //TODO
                            break;
                        case LogType.Database:
                            if (ServerStore.Logger.Loggers.TryGetSubLogger<DocumentDatabase>(out var logger))
                            {
                                djv = new DynamicJsonValue
                                {
                                    ["DatabaseLoggers"] = GetAllSubLoggers(logger.Loggers)
                                };
                            }
                            break;
                        case LogType.Index:
                            if (ServerStore.Logger.Loggers.TryGetSubLogger<DocumentDatabase>(out logger))
                            {
                                count = 0;
                                dja = new DynamicJsonArray();
                                // TODO: this foreach can be prevented by using "indexstorage" (or similar) to hold all indexes of document db (like I do with documentStore logger of serverStore 
                                foreach (var dbLogger in logger.GetLoggers())
                                {
                                    dic = new Dictionary<string, LoggingSourceHolder>();
                                    foreach (var holder in dbLogger.Value.Logger.GetLoggers())
                                    {
                                        if ((LogType)Enum.Parse(typeof(LogType), holder.Value.Type) == LogType.Index)
                                        {
                                            dic.Add(holder.Value.Source, holder.Value);
                                        }
                                    }

                                    if (dic.Count > 0)
                                    {
                                        dja.Add(new DynamicJsonValue
                                        {
                                            ["Database"] = dbLogger.Value.Source,
                                            ["Loggers"] = GetAllSubLoggers(dic),
                                            ["Count"] = dic.Count
                                        });

                                        count += dic.Count;
                                    }
                                }

                                if (dja.Count > 0)
                                {
                                    djv = new DynamicJsonValue
                                    {
                                        ["Count"] = count,
                                        ["IndexLoggers"] = dja
                                    };
                                }
                            }
                            break;
                        default:
                            throw new InvalidOperationException($"Cannot get loggers for type '{type}'.");
                    }
                }
                else
                {
                    djv = new DynamicJsonValue
                    {
                        ["AllLoggers"] = GetAllSubLoggers(LoggingSource.Instance.Loggers)
                    };
                }

                var json = context.ReadObject(djv, "logs/loggers");
                writer.WriteObject(json);
            }

            return Task.CompletedTask;
        }

        //private static DynamicJsonArray NewMethod(ConcurrentDictionary<string, LoggingSourceHolder> loggers)
        //{
        //    var dja = new DynamicJsonArray();
        //    foreach (var kvp in loggers)
        //    {
        //        if (kvp.Value.Logger.Loggers.IsEmpty == false)
        //        {
        //            var djv = new DynamicJsonValue();
        //            djv[kvp.Key] = NewMethod(kvp.Value.Logger.Loggers);
        //            dja.Add(djv);
        //        }
        //        else
        //        {
        //            dja.Add(kvp.Key);
        //        }

        //    }

        //    return dja;
        //}
        private static DynamicJsonArray GetAllSubLoggers(IEnumerable<KeyValuePair<string, LoggingSourceHolder>> loggers)
        {
            var dja = new DynamicJsonArray();
            foreach (var kvp in loggers)
            {
                var djv = new DynamicJsonValue();
                djv["Name"] = kvp.Key;
                djv[nameof(LoggingSourceHolder.Source)] = kvp.Value.Source;
                djv[nameof(LoggingSourceHolder.Type)] = kvp.Value.Type;
                djv[nameof(LoggingSourceHolder.Mode)] = kvp.Value.Mode;
                if (kvp.Value.Logger.HasLoggers() == false)
                {
                   // djv[nameof(Logger.Loggers)] = GetAllSubLoggers(kvp.Value.Logger.Loggers);
                    djv["Loggers"] = GetAllSubLoggers(kvp.Value.Logger.Loggers);
                }

                dja.Add(djv);
            }

            return dja;
        }

        private static bool SetLoggerModeByName(IEnumerable<KeyValuePair<string, LoggingSourceHolder>> loggers, string name, LogMode mode)
        {
            foreach (var kvp in loggers)
            {
                if (kvp.Key == name)
                {
                    // TODO: set the logger mode
                    kvp.Value.Logger.SetLoggerMode(mode);
                    //var logger = kvp.Value.Logger;
                    //logger.SetLoggerMode(mode);
                    //kvp.Value.Logger = logger;


                    //TODO:
                 //   var k =loggers[kvp.Key];
                    //    = new LoggingSourceHolder()
                    //{
                    //    Source = kvp.Value.Source, Type = kvp.Value.Type, Logger = kvp.Value.Logger, Mode = kvp.Value.Logger.GetLogMode()
                    //};
              //      k.Mode = mode;
                    return true;
                }

                if (kvp.Value.Logger.HasLoggers() == false)
                    continue;

                if (SetLoggerModeByName(kvp.Value.Logger.Loggers, name, mode))
                    return true;
            }

            return false;
        }

        private static bool ResetLoggerModeByName(IEnumerable<KeyValuePair<string, LoggingSourceHolder>> loggers, string name)
        {
            foreach (var kvp in loggers)
            {
                if (kvp.Key == name)
                {
                    if (kvp.Value.Logger.CanReset() == false)
                        return false;

                    kvp.Value.Logger.ResetLogger();
                    //TODO:
                    //loggers[kvp.Key] = new LoggingSourceHolder()
                    //{
                    //    Source = kvp.Value.Source,
                    //    Type = kvp.Value.Type,
                    //    Logger = kvp.Value.Logger,
                    //    Mode = kvp.Value.Logger.GetLogMode()
                    //};

                    return true;
                }

                if (kvp.Value.Logger.HasLoggers() == false)
                    continue;

                if (ResetLoggerModeByName(kvp.Value.Logger.Loggers, name))
                    return true;
            }

            return false;
        }

        //TODO: add type? 
        // /admin/loggers/reset/all?type=Index
        // /admin/loggers/reset/all?type=Cluster
        [RavenAction("/admin/loggers/reset/all", "GET", AuthorizationStatus.Operator)]
        public Task ResetAllLoggers()
        {
            foreach (var kvp in LoggingSource.Instance.Loggers)
            {
                kvp.Value.Logger.ResetLogger();
                LoggingSource.Instance.Loggers[kvp.Key] = new LoggingSourceHolder()
                {
                    Source = kvp.Value.Source,
                    Type = kvp.Value.Type,
                    Logger = kvp.Value.Logger,
                    Mode = kvp.Value.Logger.GetLogMode()
                };
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/loggers/reset", "GET", AuthorizationStatus.Operator)]
        public Task ResetLoggers()
        {
            StringValues loggers = GetStringValuesQueryString("logger", required: true);
            if (loggers.Count == 0) 
                return Task.CompletedTask;
            var test = loggers.First().ToString();
            foreach (var logger in loggers)
            {
                var success = ResetLoggerModeByName(LoggingSource.Instance.Loggers, logger);
            }

            return Task.CompletedTask;
        }

        // /admin/loggers/set?mode=type
        // /admin/loggers/set?mode=name
        [RavenAction("/admin/loggers/set", "POST", AuthorizationStatus.Operator)]
        public async Task SetLoggerMode()
        {
            var mode = GetStringQueryString("mode", required: true);

            switch (mode)
            {
                case "name":
                    using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        //TODO: put better names
                        var input = await ctx.ReadForMemoryAsync(RequestBodyStream(), "Loggers");
                        if (input.TryGet("Loggers", out BlittableJsonReaderArray loggers) == false)
                            ThrowRequiredPropertyNameInRequest("Loggers");
                        // TODO: MB pass the loggers type as well
                        foreach (BlittableJsonReaderObject bjro in loggers)
                        {
                            LoggerHolder holder = JsonDeserializationServer.LoggerHolder(bjro);
                            var success = SetLoggerModeByName(LoggingSource.Instance.Loggers, holder.Name, holder.Mode);
                        }
                    }
                    break;
                case "type":
                    break;
                default:
                    throw new InvalidOperationException($"not supported mode: '{mode}'.");
            }



            //var names = GetStringValuesQueryString("names", required: false);
            //if (names.Count == 0) // TODO: throw exception?
            //    return;

            //var mode = (LogMode)Enum.Parse(typeof(LogMode), GetStringQueryString("mode", required: true));

            //foreach (var logger in LoggingSource.Instance.Loggers)
            //{
            //    logger.Value.Logger.ResetLogger();
            //}
        }
    }

    internal class LoggerHolderParent1
    {
        public List<LoggerHolderWithName> Loggers { set; get; }
    }

    internal class LoggerHolderWithName
    {
        public string Name { set; get; }
        public LogMode Mode { set; get; }
    }

    internal class LoggerHolderParent2
    {
        public List<LoggerHolderWithType> Loggers { set; get; }
    }

    internal class LoggerHolderWithType
    {
        public LogType Type { set; get; }
        public LogMode Mode { set; get; }
    }
}

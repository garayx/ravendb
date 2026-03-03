using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Json;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.CDC.Stats.Performance;

public class LiveCdcSinkPerformanceCollector : DatabaseAwareLivePerformanceCollector<CdcSinkTaskPerformanceStats>
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>> _perCdcSinkProcessStats = new();

    public LiveCdcSinkPerformanceCollector(DocumentDatabase database, Dictionary<string, List<CdcSinkProcess>> CdcSinks) : base(database)
    {
        foreach (var sink in CdcSinks)
        {
            var processes = _perCdcSinkProcessStats.GetOrAdd(sink.Key, s => new ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>());

            foreach (var sinkProcess in sink.Value)
            {
                processes.TryAdd(sinkProcess.Script.Name, new CdcSinkProcessAndPerformanceStatsList(sinkProcess));
            }
        }

        Start();
    }

    protected override async Task StartCollectingStats()
    {
        Database.CdcSinkLoader.BatchCompleted += BatchCompleted;
        Database.CdcSinkLoader.ProcessAdded += ProcessAdded;
        Database.CdcSinkLoader.ProcessRemoved += ProcessRemoved;

        try
        {
            var stats = Client.Extensions.EnumerableExtension.ForceEnumerateInThreadSafeManner(_perCdcSinkProcessStats)
                .Select(x =>
                {
                    var result = new CdcSinkTaskPerformanceStats
                    {
                        TaskName = x.Key
                    };

                    var perfStats = new List<CdcSinkProcessPerformanceStats>();

                    foreach (var eltAndStats in x.Value)
                    {
                        var process = eltAndStats.Value.Handler;

                        perfStats.Add(new CdcSinkProcessPerformanceStats
                        {
                            ScriptName = process.Script.Name,
                            Performance = process.GetPerformanceStats()
                        });

                        result.BrokerType = process.Configuration.BrokerType;
                        result.TaskId = process.TaskId;
                    }

                    result.Stats = perfStats.ToArray();

                    return result;
                })
                .ToList();

            Stats.Enqueue(stats);

            await RunInLoop();
        }
        finally
        {
            Database.CdcSinkLoader.BatchCompleted -= BatchCompleted;
            Database.CdcSinkLoader.ProcessAdded -= ProcessAdded;
            Database.CdcSinkLoader.ProcessRemoved -= ProcessRemoved;
        }
    }

    protected override List<CdcSinkTaskPerformanceStats> PreparePerformanceStats()
    {
        var preparedStats = new List<CdcSinkTaskPerformanceStats>(_perCdcSinkProcessStats.Count);

        foreach (var taskProcesses in _perCdcSinkProcessStats)
        {
            List<CdcSinkProcessPerformanceStats> processesStats = null;

            var type = CdcBrokerType.None;
            long taskId = -1;

            foreach (var ququeSinkItem in taskProcesses.Value)
            {
                var ququeSinkAndPerformanceStatsList = ququeSinkItem.Value;
                var CdcSink = ququeSinkAndPerformanceStatsList.Handler;
                var performance = ququeSinkAndPerformanceStatsList.Performance;

                var itemsToSend = new List<CdcSinkStatsAggregator>(performance.Count);

                while (performance.TryTake(out CdcSinkStatsAggregator stats))
                {
                    itemsToSend.Add(stats);
                }

                var latestStats = CdcSink.GetLatestPerformanceStats();
                if (latestStats != null &&
                    latestStats.Completed == false &&
                    itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    if (processesStats == null)
                        processesStats = new List<CdcSinkProcessPerformanceStats>();

                    processesStats.Add(new CdcSinkProcessPerformanceStats
                    {
                        ScriptName = CdcSink.Script.Name,
                        Performance = itemsToSend.Select(item => item.ToPerformanceLiveStatsWithDetails()).ToArray()
                    });

                    type = CdcSink.Configuration.BrokerType;
                    taskId = CdcSink.TaskId;
                }
            }

            if (processesStats != null && processesStats.Count > 0)
            {
                preparedStats.Add(new CdcSinkTaskPerformanceStats
                {
                    TaskName = taskProcesses.Key,
                    TaskId = taskId,
                    BrokerType = type,
                    Stats = processesStats.ToArray()
                });
            }
        }
        return preparedStats;
    }

    protected override void WriteStats(List<CdcSinkTaskPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
    {
       writer.WriteCdcSinkTaskPerformanceStats(context, stats);
    }

    private void ProcessRemoved(CdcSinkProcess CdcSink)
    {
        if (_perCdcSinkProcessStats.TryGetValue(CdcSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryRemove(CdcSink.Script.Name, out _);
    }

    private void ProcessAdded(CdcSinkProcess CdcSink)
    {
        if (_perCdcSinkProcessStats.TryGetValue(CdcSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryAdd(CdcSink.Script.Name, new CdcSinkProcessAndPerformanceStatsList(CdcSink));
    }

    private void BatchCompleted((string ConfigurationName, string TransformationName, CdcSinkProcessStatistics Statistics) change)
    {
        if (_perCdcSinkProcessStats.TryGetValue(change.ConfigurationName, out var taskProcesses) == false)
        {
            _perCdcSinkProcessStats.TryAdd(change.ConfigurationName, taskProcesses = new ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>());
        }

        if (taskProcesses.TryGetValue(change.TransformationName, out var processAndPerformanceStats) == false)
        {
            var processes = Database.CdcSinkLoader.Processes;

            var etl = processes.FirstOrDefault(x => x.Configuration.Name.Equals(change.ConfigurationName, StringComparison.OrdinalIgnoreCase) &&
                                                    x.Script.Name.Equals(change.TransformationName, StringComparison.OrdinalIgnoreCase));

            if (etl == null)
                return;

            processAndPerformanceStats = new CdcSinkProcessAndPerformanceStatsList(etl);

            taskProcesses.TryAdd(change.TransformationName, processAndPerformanceStats);
        }

        var latestStat = processAndPerformanceStats.Handler.GetLatestPerformanceStats();
        if (latestStat != null)
            processAndPerformanceStats.Performance.Add(latestStat);
    }

    private class CdcSinkProcessAndPerformanceStatsList : HandlerAndPerformanceStatsList<CdcSinkProcess, CdcSinkStatsAggregator>
    {
        public CdcSinkProcessAndPerformanceStatsList(CdcSinkProcess CdcSink) : base(CdcSink)
        {
            TaskId = CdcSink.TaskId;
        }

        public long TaskId { get; }
    }
}

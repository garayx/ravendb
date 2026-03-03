using Raven.Client.Documents.Operations.ETL.Queue;

namespace Raven.Server.Documents.CDC.Stats.Performance;

public class CdcSinkTaskPerformanceStats
{
    public long TaskId { get; set; }

    public string TaskName { get; set; }

    public CdcBrokerType BrokerType { get; set; }

    public CdcSinkProcessPerformanceStats[] Stats { get; set; }
}

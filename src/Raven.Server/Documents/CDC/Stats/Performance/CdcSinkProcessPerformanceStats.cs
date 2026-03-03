namespace Raven.Server.Documents.CDC.Stats.Performance;

public class CdcSinkProcessPerformanceStats
{
    public string ScriptName { get; set; }
    public CdcSinkPerformanceStats[] Performance { get; set; }
}

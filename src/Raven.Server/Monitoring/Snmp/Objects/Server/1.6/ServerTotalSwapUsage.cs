using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalSwapUsage : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;

        public ServerTotalSwapUsage(MetricCacher metricCacher)
            : base(SnmpOids.Server.TotalSwapUsage)
        {
            _metricCacher = metricCacher;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(_metricCacher.GetValue<MemoryInfoResult>(
                MetricCacher.Keys.Server.MemoryInfoExtended).TotalSwapUsage.GetValue(SizeUnit.Megabytes));
        }
    }
}

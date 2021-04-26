using System;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents
{
    public class DatabaseMetricCacher : MetricCacher
    {
        private readonly DocumentDatabase _database;
        private readonly Logger _logger;

        public DatabaseMetricCacher(DocumentDatabase database)
        {
            _database = database;
            _logger = _database._logger.GetLoggerFor(nameof(DatabaseMetricCacher), LogType.Database);
        }

        public void Initialize()
        {
            Register(MetricCacher.Keys.Database.DiskSpaceInfo, TimeSpan.FromSeconds(30), CalculateDiskSpaceInfo);
        }

        private DiskSpaceResult CalculateDiskSpaceInfo()
        {
            return DiskSpaceChecker.GetDiskSpaceInfo(_database.Configuration.Core.DataDirectory.FullPath, _logger);
        }
    }
}

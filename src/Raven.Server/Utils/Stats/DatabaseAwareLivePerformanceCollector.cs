using Raven.Server.Documents;
using Sparrow.Logging;

namespace Raven.Server.Utils.Stats
{
    public abstract class DatabaseAwareLivePerformanceCollector<T> : LivePerformanceCollector<T>
    {
        protected readonly DocumentDatabase Database;

        protected DatabaseAwareLivePerformanceCollector(DocumentDatabase database): base(database.DatabaseShutdown, database._logger.GetLoggerFor(nameof(LivePerformanceCollector<T>), LogType.Database))
        {
            Database = database;
        }
        
    }
}

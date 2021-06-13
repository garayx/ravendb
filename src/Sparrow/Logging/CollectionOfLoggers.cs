using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Logging
{
    public class CollectionOfLoggers : IEnumerable<KeyValuePair<string, LoggingSourceHolder>>, IDisposable
    {
        private readonly ConcurrentDictionary<string, LoggingSourceHolder> _loggers = new ConcurrentDictionary<string, LoggingSourceHolder>(StringComparer.OrdinalIgnoreCase);

        public CollectionOfLoggers()
        {
        }

        public CollectionOfLoggers(CollectionOfLoggers collectionOfLoggers)
        {
            _loggers = new ConcurrentDictionary<string, LoggingSourceHolder>(collectionOfLoggers);
        }

        // todo: Split source & type
        public Logger Add(LoggingSource loggingSource, Logger parent, string name, LogType type)
        {
            var holder = _loggers.GetOrAdd(name, _ =>
                {
                    string source;
                    LogMode mode; 
                    if (parent == null)
                    {
                        source = "N/A";
                        mode = loggingSource.LogMode;
                    }
                    else
                    {
                        source = parent._logger;
                        mode = parent.GetLogMode();
                    }

                    var logger = new Logger(loggingSource, parent: parent, source, name, type);
                    return new LoggingSourceHolder { Source = source, Type = type, Logger = logger, Mode = mode};
                }
               );

            return holder.Logger;
        }

        public void Remove(string name)
        {
            if (_loggers.TryRemove(name, out var holder))
                holder.Logger.Dispose();
        }

        public void UpdateMode(string name, LogMode mode)
        {
            _loggers[name].Mode = mode;
        }

        public IEnumerator<KeyValuePair<string, LoggingSourceHolder>> GetEnumerator()
        {
            return _loggers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool TryGetSubLogger<T>(out Logger logger)
        {
            return TryGetSubLogger(typeof(T).Name, out logger);
        }

        public bool TryGetSubLogger(string name, out Logger logger)
        {
            foreach (var holder in _loggers)
            {
                if (holder.Key != name)
                    continue;

                logger = holder.Value.Logger;
                return true;
            }

            logger = null;
            return false;
        }

        //public bool ReplaceOrAdd(string old, string newLogger)
        //{
        //    if (_loggers.ContainsKey(old))
        //    {
        //        if (_loggers.ContainsKey(newLogger))
        //        {
        //            if(_loggers.TryRemove(old, out var logger))
        //                logger.Logger.Dispose();
        //        }
        //        else
        //        {
        //            if (_loggers.TryRemove(old, out var logger))
        //            {
        //                Add(logger.Source, )
        //            }
        //        }
        //    }

        //    foreach (var holder in _loggers)
        //    {
        //        if (holder.Key != old)
        //            continue;

        //        if (_loggers.ContainsKey(newLogger))
        //        {

        //        }
        //        logger = holder.Value.Logger;
        //        return true;
        //    }

        //    return true;
        //}

        public void Dispose()
        {
            foreach (var kvp in _loggers)
            {
                kvp.Value.Logger.Dispose();
            }
        }

        public int Count => _loggers.Count;

        public bool TryRemove(string safeName, out LoggingSourceHolder o)
        {
            return _loggers.TryRemove(safeName, out o);
        }
    }
}

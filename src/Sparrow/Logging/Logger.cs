using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Logging
{
    public class Logger : IDisposable
    {
        public LogType Type { get; }

        private readonly LoggingSource _loggingSource;
        private readonly string _source;
        internal readonly string _logger;
        internal readonly Logger _parent;

        [ThreadStatic]
        private static LogEntry _logEntry;

        public Logger(LoggingSource loggingSource, string source, string logger, LogType type)
        {
            _loggingSource = loggingSource;
            _source = source;
            _logger = logger;
            Type = type;
        }

        public Logger(LoggingSource loggingSource, Logger parent, string source, string logger, LogType type)
        {
            _loggingSource = loggingSource;
            _source = source;
            _logger = logger;
            _parent = parent;
            Type = type;
        }

        private bool? _isOperationsEnabled;
        private bool? _isInfoEnabled;

        public void Info(FormattableString msg, Exception e = null)
        {
            try
            {
                var msgStr = msg.ToString();
                Info(msgStr, e);
            }
            catch (Exception)
            {
                Info(msg.Format, e);
            }
        }

        public void Operations(FormattableString msg, Exception e = null)
        {
            try
            {
                var msgStr = msg.ToString();
                Operations(msgStr, e);
            }
            catch (Exception)
            {
                Info(msg.Format, e);
            }
        }

        public void Info(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Information;
            _loggingSource.Log(ref _logEntry);
        }

        public Task InfoWithWait(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Information;

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _loggingSource.Log(ref _logEntry, tcs);

            return tcs.Task;
        }

        public void Operations(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Operations;
            _loggingSource.Log(ref _logEntry);
        }

        public Task OperationsWithWait(string msg, Exception ex = null)
        {
            _logEntry.At = GetLogDate();
            _logEntry.Exception = ex;
            _logEntry.Logger = _logger;
            _logEntry.Message = msg;
            _logEntry.Source = _source;
            _logEntry.Type = LogMode.Operations;

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _loggingSource.Log(ref _logEntry, tcs);

            return tcs.Task;
        }

        public bool IsInfoEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _isInfoEnabled ?? _loggingSource.IsInfoEnabled; }
        }

        public bool IsOperationsEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _isOperationsEnabled ?? _loggingSource.IsOperationsEnabled; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static DateTime GetLogDate()
        {
            var now = DateTime.UtcNow;
            if (LoggingSource.UseUtcTime == false)
                now = new DateTime(now.Ticks + LoggingSource.LocalToUtcOffsetInTicks);

            return now;
        }

        // TODO: remove
        //  public ConcurrentDictionary<string, LoggingSourceHolder> Loggers = new ConcurrentDictionary<string, LoggingSourceHolder>(StringComparer.OrdinalIgnoreCase);

        public CollectionOfLoggers Loggers = new CollectionOfLoggers();

        public Logger GetLoggerFor(string name, LogType type)
        {
            if (string.IsNullOrEmpty(name))
                throw new InvalidOperationException($"{nameof(name)} {nameof(string.IsNullOrEmpty)}");

            return Loggers.Add(_loggingSource, parent: this, name, type);
        }

        public Logger GetLoggerFor<T>(LogType type)
        {
           return GetLoggerFor(typeof(T).Name, type);
        }

        public void TryRemoveLogger(string name)
        {
            Loggers.Remove(name);
        }

        public IEnumerable<KeyValuePair<string, LoggingSourceHolder>> GetLoggers()
        {
            return Loggers;
        }

        public bool HasLoggers()
        {
            return Loggers.Count > 0;
        }
        //TODO what return etc

        //public Logger GetLoggerFor<T>(string name, LogType source)
        //{
        //    if (source != LogType.Database)
        //        throw new InvalidOperationException("GetLoggerFor<T>() supported for LogType.Database only.");

        //    if (string.IsNullOrEmpty(name))
        //        throw new InvalidOperationException($"{nameof(name)} {nameof(string.IsNullOrEmpty)}");


        //    var holder = Loggers.GetOrAdd(name, new LoggingSourceHolder 
        //        { 
        //            Source = name, 
        //            Type = source.ToString(), 
        //            Logger = new Logger(_loggingSource, parent: this, source.ToString(), typeof(T).FullName, source)

        //        });

        //    return holder.Logger;
        //}

        //public LogMode GetLogMode()
        //{
        //    return _isOperationsEnabled switch
        //    {
        //        null when _isInfoEnabled == null => _loggingSource.LogMode,
        //        null => LogMode.Information,
        //        _ => _isInfoEnabled == null ? LogMode.Operations : LogMode.Information
        //    };
        //}

        public LogMode GetLogMode()
        {
            LogMode mode;
            if (_isOperationsEnabled == null && _isInfoEnabled == null)
            {
                mode = _loggingSource.LogMode;
            }
            else if (_isOperationsEnabled == null)
            {
                if (_isInfoEnabled.Value)
                    mode = LogMode.Information;
                else
                    mode = LogMode.None; // TODO ???
            }
            else if (_isInfoEnabled == null)
            {
                if (_isOperationsEnabled.Value)
                    mode = LogMode.Operations;
                else
                    mode = LogMode.None; // TODO ???
            }
            else
            {
                if (_isOperationsEnabled.Value && _isInfoEnabled.Value)
                    mode = LogMode.Information;
                else if (_isOperationsEnabled.Value)
                    mode = LogMode.Operations;
                else if (_isInfoEnabled.Value)
                    mode = LogMode.Information;
                else
                    mode = LogMode.None;
            }

            return mode;
        }

        public void SetLoggerMode(LogMode mode)
        {
            switch (mode)
            {
                case LogMode.None:
                    _isOperationsEnabled = false;
                    _isInfoEnabled = false;
                    break;
                case LogMode.Information:
                    _isInfoEnabled = true;
                    _isOperationsEnabled = false; // TODO: is it right ?
                    break;
                case LogMode.Operations:
                    _isOperationsEnabled = true;
                    _isInfoEnabled = false;
                    break;
                default:
                    throw new InvalidOperationException($"Invalid {nameof(LogMode)}: '{mode}'");
            }

            foreach (var kvp in Loggers)
            {
                kvp.Value.Logger.SetLoggerMode(mode);
                Loggers.UpdateMode(kvp.Key, mode);

                //Loggers[kvp.Key] = new LoggingSourceHolder()
                //{
                //    Source = kvp.Value.Source,
                //    Type = kvp.Value.Type,
                //    Logger = kvp.Value.Logger,
                //   // Mode = kvp.Value.Logger.GetLogMode()
                //    Mode = mode
                //};
            }
        }
        public bool CanReset()
        {
            return _isOperationsEnabled != null || _isInfoEnabled != null;
        }

        public void ResetLogger()
        {
            if (_isOperationsEnabled == null && _isInfoEnabled == null)
                return;

            _isOperationsEnabled = null;
            _isInfoEnabled = null;

            foreach (var kvp in Loggers)
            {
                kvp.Value.Logger.ResetLogger();
                Loggers.UpdateMode(kvp.Key, LogMode.None);

                //Loggers[kvp.Key] = new LoggingSourceHolder()
                //{
                //    Source = kvp.Value.Source,
                //    Type = kvp.Value.Type,
                //    Logger = kvp.Value.Logger,
                //    Mode = kvp.Value.Logger.GetLogMode()
                //};
            }
        }

        public static string GetNameFor(string type, string path)
        {
            return $"{type}: '{path}'";
        }

        public void Dispose()
        {
            // try remove from parent list
         //   _parent.Loggers.TryRemove(_logger, out _);
            _parent.TryRemoveLogger(this._logger);

            // remove childs
            //foreach (var logger in Loggers)
            //{
            //    logger.Value.Logger.Dispose();
            //}
            Loggers.Dispose();

            // remove myself
        }
    }
}

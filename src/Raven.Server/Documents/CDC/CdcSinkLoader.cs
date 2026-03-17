using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CDC;

public class CdcSinkLoader : IDisposable
{
    private const string AlertTitle = "Cdc Sink loader";

    private CdcSinkProcess[] _processes = new CdcSinkProcess[0];

    private readonly HashSet<string> _uniqueConfigurationNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private DatabaseRecord _databaseRecord;
    private readonly object _loadProcessedLock = new object();
    private readonly DocumentDatabase _database;
    private readonly ServerStore _serverStore;
    protected RavenLogger Logger;
    public CdcSinkProcess[] Processes => _processes;

    public event Action<(string ConfigurationName, string ScriptName, CdcSinkProcessStatistics Statistics)> BatchCompleted;

    public void OnBatchCompleted(string configurationName, string scriptName, CdcSinkProcessStatistics statistics)
    {
        BatchCompleted?.Invoke((configurationName, scriptName, statistics));
    }

    public event Action<CdcSinkProcess> ProcessAdded;

    public event Action<CdcSinkProcess> ProcessRemoved;

    public List<CdcSinkConfiguration> Sinks;

    public void Initialize(DatabaseRecord record)
    {
        LoadProcesses(record, record.CdcSinks, toRemove: null);
    }

    public CdcSinkLoader() { }

    public CdcSinkLoader(DocumentDatabase documentDatabase, ServerStore serverStore)
    {
        _database = documentDatabase;
        _serverStore = serverStore;
        Logger = documentDatabase.Loggers.GetLogger(GetType());
    }

    private void LoadProcesses(DatabaseRecord record, List<CdcSinkConfiguration> newCdcSinkDestinations,
        List<CdcSinkProcess> toRemove)
    {
        lock (_loadProcessedLock)
        {
            _databaseRecord = record;

            Sinks = _databaseRecord.CdcSinks;

            var processes = new List<CdcSinkProcess>(_processes);

            if (toRemove != null && toRemove.Count > 0)
            {
                foreach (var process in toRemove)
                {
                    processes.Remove(process);
                    _uniqueConfigurationNames.Remove(process.Configuration.Name);

                    OnProcessRemoved(process);
                }
            }

            var ensureUniqueConfigurationNames = _uniqueConfigurationNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

            var newProcesses = new List<CdcSinkProcess>();
            if (newCdcSinkDestinations != null && newCdcSinkDestinations.Count > 0)
                newProcesses.AddRange(
                    GetRelevantProcesses<CdcSinkConfiguration, CdcConnectionString>(newCdcSinkDestinations,
                        ensureUniqueConfigurationNames));

            processes.AddRange(newProcesses);
            _processes = processes.ToArray();

            foreach (var process in newProcesses)
            {
                process.Start();

                OnProcessAdded(process);

                _uniqueConfigurationNames.Add(process.Configuration.Name);
            }
        }
    }

    private IEnumerable<CdcSinkProcess> GetRelevantProcesses<T, TConnectionString>(List<T> configurations,
        HashSet<string> uniqueNames) where T : CdcSinkConfiguration where TConnectionString : ConnectionString
    {
        foreach (var config in configurations)
        {
            var connectionStringNotFound = false;

            CdcSinkConfiguration CdcSinkConfig = config;
            if (_databaseRecord.CdcConnectionStrings.TryGetValue(config.ConnectionStringName, out var CdcConnection))
                CdcSinkConfig.Initialize(CdcConnection);
            else
                connectionStringNotFound = true;

            if (connectionStringNotFound)
            {
                LogConfigurationError(config,
                    new List<string> { $"Connection string named '{config.ConnectionStringName}' was not found." });

                continue;
            }

            if (ValidateConfiguration(config, uniqueNames) == false)
                continue;

            CdcSinkProcessState processState = GetProcessState(config.Scripts, _database, config.Name);
            var whoseTaskIsIt = OngoingTasksUtils.WhoseTaskIsIt(_serverStore, _databaseRecord.Topology, config, processState, _database.NotificationCenter);
            if (whoseTaskIsIt != _serverStore.NodeTag)
                continue;

            // TODO: egor this should be same as GenericDatabaseMigrator.Migrate method, we need to iterate over the collections ?
            //foreach (var transform in config.Scripts)
            //{
            //    CdcSinkProcess process = CdcSinkProcess.CreateInstance(transform, config, _database);
            //    yield return process;
            //}

            CdcSinkProcess process = CdcSinkProcess.CreateInstance(processState, config, _database);
            yield return process;
        }
    }

    private bool ValidateConfiguration(CdcSinkConfiguration config, HashSet<string> uniqueNames)
    {
        if (config.Validate(out List<string> errors) == false)
        {
            LogConfigurationError(config, errors);
            return false;
        }

        if (uniqueNames.Add(config.Name) == false)
        {
            LogConfigurationError(config,
                new List<string> { $"Cdc Sink with name '{config.Name}' is already defined" });
            return false;
        }

        return true;
    }

    private void OnProcessRemoved(CdcSinkProcess process)
    {
        ProcessRemoved?.Invoke(process);
    }

    private void OnProcessAdded(CdcSinkProcess process)
    {
        ProcessAdded?.Invoke(process);
    }

    public virtual void Dispose()
    {
        var ea = new ExceptionAggregator(Logger, "Could not dispose Cdc Sink loader");

        Parallel.ForEach(_processes, x => ea.Execute(x.Dispose));

        ea.ThrowIfNeeded();
    }

    private bool IsMyCdcSinkTask<T, TConnectionString>(DatabaseRecord record, T CdcSinkTask,
        ref Dictionary<string, string> responsibleNodes)
        where TConnectionString : ConnectionString
        where T : CdcSinkConfiguration
    {
        var processState = GetProcessState(CdcSinkTask.Scripts, _database, CdcSinkTask.Name);
        var whoseTaskIsIt = OngoingTasksUtils.WhoseTaskIsIt(_serverStore, record.Topology, CdcSinkTask, processState, _database.NotificationCenter);

        responsibleNodes[CdcSinkTask.Name] = whoseTaskIsIt;

        return whoseTaskIsIt == _serverStore.NodeTag;
    }

    public static CdcSinkProcessState GetProcessState(List<CdcSinkScript> scripts, DocumentDatabase database,
        string configurationName)
    {
        CdcSinkProcessState processState = null;

        processState = CdcSinkProcess.GetProcessState(database, configurationName);

        return processState ?? new CdcSinkProcessState();
    }

    private void LogConfigurationError(CdcSinkConfiguration config, List<string> errors)
    {
        var errorMessage =
            $"Invalid Cdc Sink configuration for '{config.Name}'{(config.Connection != null ? $" ({config.GetDestination()})" : string.Empty)}. " +
            $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

        if (Logger.IsInfoEnabled)
            Logger.Info(errorMessage);

        var alert = AlertRaised.Create(_database.Name, AlertTitle, errorMessage, AlertReason.CdcSink_Error, NotificationSeverity.Error);

        _database.NotificationCenter.Add(alert);
    }

    private static string GetStopReason(
        CdcSinkProcess process,
        DatabaseRecord record,
        List<CdcSinkConfiguration> myCdcSink,
        Dictionary<string, string> responsibleNodes)
    {
        CdcSinkConfigurationCompareDifferences? differences = null;
        var transformationDiffs =
            new List<(string TransformationName, CdcSinkConfigurationCompareDifferences Difference)>();

        var reason = "Database record change. ";

        if (process is not null)
        {
            var existing = myCdcSink.FirstOrDefault(x =>
                x.Name.Equals(process.Configuration.Name, StringComparison.OrdinalIgnoreCase));

            if (existing != null)
                differences = process.Configuration.Compare(existing, record.CdcConnectionStrings, transformationDiffs);
        }
        else
        {
            throw new InvalidOperationException($"Unknown Cdc Sink process type: " + process.GetType().FullName);
        }

        if (differences != null)
        {
            reason += $"Configuration changes: {differences}. Details: ";

            foreach (var transformationDiff in transformationDiffs)
            {
                reason += $"Script '{transformationDiff.TransformationName}' - {transformationDiff.Difference}. ";
            }
        }
        else
        {
            if (responsibleNodes.TryGetValue(process.Configuration.Name, out var responsibleNode))
            {
                reason += $"Cdc Sink was moved to another node. Responsible node is: {responsibleNode}";
            }
            else
            {
                reason +=
                    $"Cdc Sink was deleted or moved to another node (no configuration named '{process.Configuration.Name}' was found). ";
            }
        }

        return reason;
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        var myCdcSink = new List<CdcSinkConfiguration>();
        var responsibleNodes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var config in record.CdcSinks)
        {
            if (IsMyCdcSinkTask<CdcSinkConfiguration, CdcConnectionString>(record, config, ref responsibleNodes))
            {
                myCdcSink.Add(config);
            }
        }

        var toRemove = _processes.GroupBy(x => x.Configuration.Name).ToDictionary(x => x.Key, x => x.ToList());

        foreach (var processesPerConfig in _processes.GroupBy(x => x.Configuration.Name))
        {
            var process = processesPerConfig.First();

            Debug.Assert(processesPerConfig.All(x => x.GetType() == process.GetType()));

            CdcSinkConfiguration existing = null;

            foreach (var config in myCdcSink)
            {
                var diff = process.Configuration.Compare(config, record.CdcConnectionStrings);

                if (diff == CdcSinkConfigurationCompareDifferences.None)
                {
                    existing = config;
                    break;
                }
            }

            if (existing != null)
            {
                toRemove.Remove(processesPerConfig.Key);
                myCdcSink.Remove(existing);
            }
        }

        LoadProcesses(record, myCdcSink, toRemove.SelectMany(x => x.Value).ToList());

        if (toRemove.Count == 0)
            return;

        ThreadPool.QueueUserWorkItem(_ =>
        {
            Parallel.ForEach(toRemove, x =>
            {
                foreach (var process in x.Value)
                {
                    try
                    {
                        if (_database.DatabaseShutdown.IsCancellationRequested)
                            return;

                        using (process)
                        {
                            string reason = GetStopReason(process, record, myCdcSink, responsibleNodes);
                            process.Stop(reason);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsErrorEnabled)
                            Logger.Error(
                                $"Failed to dispose Cdc sink process {process.Name} on the database record change", e);
                    }
                }
            });
        });
    }

    public long GetSinkCountByBroker(CdcBrokerType brokerType)
    {
        var items = Sinks.Where(x => x.BrokerType == brokerType);
        return items.Count();
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.CDC.Commands;
using Raven.Server.Documents.CDC.Stats;
using Raven.Server.Documents.CDC.Stats.Performance;
using Raven.Server.Documents.CDC.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.QueueSink.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands.CDC;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.CDC;

public abstract class CdcSinkProcess : IDisposable, ILowMemoryHandler
{
    internal const string PostgreSqlTag = "PostgreSQL Sink"; 

    private const int MinBatchSize = 16;

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

    private NativeMemory.ThreadStats _threadAllocations;
    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
    private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

    protected readonly RavenLogger Logger;

    private int _statsId;
    private CdcSinkStatsAggregator _lastStats;

    private readonly ConcurrentQueue<CdcSinkStatsAggregator> _lastCdcSinkStats = new();

    private ICdcSinkConsumer _consumer;

    protected CdcSinkProcess(CdcSinkConfiguration configuration, CdcSinkScript script,
        DocumentDatabase database, string tag)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        Logger = database.Loggers.GetLogger(GetType());
        Database = database;
        Configuration = configuration;
        Script = script;
        Tag = tag;
        Name = $"{Configuration.Name}/{Script.Name}";
        Statistics = new CdcSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
    }

    public static CdcSinkProcess CreateInstance(ulong lastLsn, CdcSinkScript script, CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        switch (configuration.BrokerType)
        {
            case CdcBrokerType.PostgreSQL:
                return new PostgresqlCdcSink(configuration, lastLsn, script, database, PostgreSqlTag);
            default:
                throw new NotSupportedException($"Unknown broker type: {configuration.BrokerType}");
        }
    }

    protected CancellationToken CancellationToken => _cts.Token;

    protected string GroupId => $"{Database.DatabaseGroupId}/{Name}";

    public DocumentDatabase Database { get; }

    
    public CdcSinkProcessStatistics Statistics { get; }

    public long TaskId => Configuration.TaskId;

    public string Tag { get; }

    public string Name { get; }

    public CdcSinkConfiguration Configuration { get; }

    public CdcSinkScript Script { get; }

    public TimeSpan? FallbackTime { get; protected set; }

    public OngoingTaskConnectionStatus GetConnectionStatus()
    {
        if (Configuration.Disabled || CancellationToken.IsCancellationRequested)
            return OngoingTaskConnectionStatus.NotActive;

        if (FallbackTime != null)
            return OngoingTaskConnectionStatus.Reconnect;

        if (Statistics.WasLatestConsumeSuccessful || Statistics.ConsumeErrors == 0)
            return OngoingTaskConnectionStatus.Active;

        return OngoingTaskConnectionStatus.NotActive;
    }

    public static CdcSinkProcessState GetProcessState(DocumentDatabase database, string configurationName,
        string scriptName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var stateBlittable = database.ServerStore.Cluster.Read(context,
                CdcSinkProcessState.GenerateItemName(database.Name, configurationName, scriptName));

            if (stateBlittable != null)
            {
                return JsonDeserializationClient.CdcSinkProcessState(stateBlittable);
            }

            return new CdcSinkProcessState();
        }
    }

    protected void UpdateProcessState(CdcSinkProcessState state)
    {
        var command = new UpdateCdcSinkProcessStateCommand(Database.Name, state, Database.ServerStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());

        var sendToLeaderTask = Database.ServerStore.SendToLeaderAsync(command);

        sendToLeaderTask.Wait(CancellationToken);
        var (etag, _) = sendToLeaderTask.Result;

        Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, Database.ServerStore.Engine.OperationTimeout).Wait(CancellationToken);
    }

    private async Task RunAsync()
    {
        while (true)
        {
            using var _ = Database.PreventFromUnloadingByIdleOperations();
            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (FallbackTime != null)
            {
                if (CancellationToken.WaitHandle.WaitOne(FallbackTime.Value))
                    return;

                FallbackTime = null;
            }

            EnsureThreadAllocationStats();

            try
            {
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    // handle intitial load
                    await HandleInitialLoadAsync();
                }

                // this is logical replication stage:
                if (_consumer == null)
                {
                    try
                    {
                        _consumer = await CreateConsumerAsync();
                    }
                    catch (Exception e)
                    {
                        string msg = $"[{Name}] Failed to create Cdc consumer";

                        if (Logger.IsErrorEnabled)
                            Logger.Error(msg, e);

                        var key = $"{Tag}/{Name}";

                        var alert = AlertRaised.Create(Database.Name, Tag, msg, AlertReason.CdcSink_ConsumerCreationError, NotificationSeverity.Error, key, new ExceptionDetails(e));

                        Database.NotificationCenter.Add(alert);

                        EnterFallbackMode();
                        continue;
                    }
                }

                var statsAggregator = new CdcSinkStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);

                using (Statistics.NewBatch())
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var stats = statsAggregator.CreateScope())
                {
                    var messages = new List<BlittableJsonReaderObject>();
                    NpgsqlLogSequenceNumber lastLsn;
                    using (var readScope = stats.For(CdcSinkBatchPhases.CdcReading, start: false))
                    {
                        var batchStarted = false;

                        while (true)
                        {
                            try
                            {
                                var message = await _consumer.ConsumeAsync(CancellationToken);

                                if (message is null)
                                    break;

                                if (batchStarted == false)
                                {
                                    statsAggregator.Start();
                                    stats.Start();
                                    readScope.Start();

                                    AddPerformanceStats(statsAggregator);
                                }

                                batchStarted = true;


                                var (id, json) = ProcessBatchItem(message);

                                if (id == "CommitMessage")
                                {
                                     lastLsn = (NpgsqlLogSequenceNumber)json;

                                    Console.WriteLine();




                                    if (CanContinueBatch(stats, messages.Count, context) == false)
                                    {
                                        break;
                                    }
                                }
                                else if (id == "BeginMessage")
                                {

                                }
                                else
                                {
                                    var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(json, context);

                                    messages.Add(blittable);

                                    readScope.RecordReadMessage();
                                }


                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                string msg = "Failed to consume message.";

                                if (Logger.IsErrorEnabled)
                                    Logger.Error(msg, e);

                                readScope.RecordReadError();
                                Statistics.RecordConsumeError(e.Message);

                                if (batchStarted == false)
                                {
                                     //failed to consume any message, let's do the fallback then
                                    EnterFallbackMode();
                                }
                            }
                        }
                    }

                    if (messages.Count == 0)
                    {
                        // empty batch, nothing to process, let's skip the script execution and just update the stats and state
                    }
                    else
                    {
                        var processedSuccessfully = 0;
                        try
                        {
                            using (var scriptProcessingScope = stats.For(CdcSinkBatchPhases.ScriptProcessing))
                            {
                                try
                                {
                                    var command = new BatchCdcSinkScriptCommand(Script.Script, messages, scriptProcessingScope, Statistics, Logger);

                                    Database.TxMerger.EnqueueSync(command);

                                    //var clusterCmd = AckLsnToCLsuter();

                                    //TODO: egor here I need to update the PostgresqlCdcSink.LastLsn 
                                    // I should  use cluster command 
                                    // need to handle cases Database.TxMerger command succeed, cluster command failed 
                                    // then I only need to apply cluster command

                                    // will we receive the same batch?
                                    // todo: egor check etl process
                                    processedSuccessfully = command.ProcessedSuccessfully;

                                    _consumer.Commit();
                                }
                                catch (JavaScriptParseException e)
                                {
                                    HandleScriptParseException(e);
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                        catch (Exception e)
                        {
                            var message = $"{Tag} Exception in Cdc sink process '{Name}'";

                            if (Logger.IsErrorEnabled)
                                Logger.Error(message, e);
                        }

                        statsAggregator.Complete();

                        if (processedSuccessfully > 0)
                        {
                            Statistics.ConsumeSuccess(processedSuccessfully);

                            try
                            {
                                UpdateProcessState(new CdcSinkProcessState
                                {
                                    ConfigurationName = Configuration.Name,
                                    ScriptName = Script.Name,
                                    NodeTag = Database.ServerStore.NodeTag
                                });

                                Database.CdcSinkLoader.OnBatchCompleted(Configuration.Name, Script.Name, Statistics);
                            }
                            catch (Exception e)
                            {
                                if (CancellationToken.IsCancellationRequested == false)
                                {
                                    if (Logger.IsErrorEnabled)
                                        Logger.Error($"{Tag} Failed to update state of Cdc sink process '{Name}'", e);
                                }
                            }
                        }
                    }
                  
                }
            }
            catch (Exception e)
            {
                var msg = $"Unexpected error in {Tag} process: '{Name}'";

                if (Logger.IsErrorEnabled)
                {
                    Logger.Error(msg, e);
                }
            }
            finally
            {
                _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            }
        }
    }


    private (string, object) ProcessBatchItem(PgOutputReplicationMessage message)
    {
        switch (message)
        {
            case InsertMessage insert:
                {
                    var (id, doc) = GetRowData(insert.Relation, insert.NewRow).GetAwaiter().GetResult();
                    //  _settings.TablesProcessingScripts.TryGetValue(insert.Relation.RelationName, out var script);
                    //   pending.Add(new ChangeRecord(id, doc, false, script));
                    return (id, doc);
                    break;
                }
            case UpdateMessage update:
                {
                    var (id, doc) =  GetRowData(update.Relation, update.NewRow).GetAwaiter().GetResult();
                    return (id, doc);
                    //    _settings.TablesProcessingScripts.TryGetValue(update.Relation.RelationName, out var script);
                                     //    pending.Add(new ChangeRecord(id, doc, false, script));
                    break;
                }
            case KeyDeleteMessage keyDel:
                {
                    var (id, doc) =  GetRowData(keyDel.Relation, keyDel.Key).GetAwaiter().GetResult();
                    //  _settings.TablesDeletionScripts.TryGetValue(keyDel.Relation.RelationName, out var script);
                    //  pending.Add(new ChangeRecord(id, doc, true, script));
                    return (id, doc);
                }
                break;
            case FullDeleteMessage fullDel:
                {
                    var (id, doc) =  GetRowData(fullDel.Relation, fullDel.OldRow).GetAwaiter().GetResult();
                    // _settings.TablesDeletionScripts.TryGetValue(fullDel.Relation.RelationName, out var script);
                    //  pending.Add(new ChangeRecord(id, doc, true, script));
                    return (id, doc);
                }
            case BeginMessage: // begin tx
                return ("BeginMessage", null);

            case CommitMessage commit:
                //    batch.AddRange(pending);
                //   pending.Clear();
                //if (lastBatch.IsCompleted)
                //{
                //    await lastBatch;
                //    conn.SetReplicationStatus(lastLsn);
                //    if (batch.Count > 0)
                //    {
                //        lastBatch = ProcessBatch(batch, config, cancellationToken);
                //        lastLsn = commit.CommitLsn;
                //        batch = [];
                //    }
                //}
                //else if (batch.Count >= _settings.BatchSize)
                //{
                //    // If previous batch is still processing, 
                //    // wait for it to complete before sending more
                //    await lastBatch;
                //    conn.SetReplicationStatus(lastLsn);
                //    lastBatch = ProcessBatch(batch, config, cancellationToken);
                //    lastLsn = commit.CommitLsn;
                //    batch = [];
                //}
                NpgsqlLogSequenceNumber lastLsn = commit.CommitLsn;
                return ("CommitMessage", lastLsn);
            default:
                throw new InvalidOperationException($"Unsupported message type: {message.GetType().Name}");
        }

    }
    private async Task<(string, object)> GetRowData(RelationMessage relation, ReplicationTuple row)
    {
        string id = relation.RelationName + "/";
        var doc = new Dictionary<string, object?>();
        bool isFirstKey = true;
        await foreach (var item in row)
        {
            var columnName = item.GetFieldName();
            object val = await item.Get();
            if (relation.Columns.Single(x => x.ColumnName == columnName)
                .Flags.HasFlag(RelationMessage.Column.ColumnFlags.PartOfKey))
            {
                if (isFirstKey)
                {
                    id += val;
                    isFirstKey = false;
                }
                else
                {
                    id += "," + val;
                }
            }
            doc[columnName] = ConvertPostgresType(item.GetDataTypeName(), item.IsDBNull ? null : val);
        }
        doc["@metadata"] = new Dictionary<string, object>
        {
            ["@collection"] = relation.RelationName
        };
        return (id, doc);
    }

    private static object? ConvertPostgresType(string postgresType, object? value)
    {
        if (value is null || value == DBNull.Value)
            return null;

        return postgresType.ToLower() switch
        {
            // Integer types
            "smallint" => Convert.ToInt16(value),
            "integer" => Convert.ToInt32(value),
            "bigint" => Convert.ToInt64(value),
            "serial" => Convert.ToInt32(value),
            "bigserial" => Convert.ToInt64(value),

            // Floating point types
            "real" => Convert.ToSingle(value),
            "double precision" => Convert.ToDouble(value),
            "numeric" => Convert.ToDecimal(value),
            "decimal" => Convert.ToDecimal(value),

            // Date/Time types
            "date" => Convert.ToDateTime(value),
            "time" => value.ToString(), // Keep as string for time-only
            "timetz" => value.ToString(), // Keep as string for time with timezone
            "timestamp" => Convert.ToDateTime(value),
            "timestamptz" => Convert.ToDateTime(value),

            // Boolean
            "boolean" => Convert.ToBoolean(value),

            // Text types
            "character" => value.ToString(),
            "character varying" => value.ToString(),
            "varchar" => value.ToString(),
            "text" => value.ToString(),

            // Binary types
            "bytea" => value, // Keep as byte array

            // UUID
            "uuid" => value.ToString(),

            // JSON types (PostgreSQL 9.2+)
            "json" => value.ToString(),
            "jsonb" => value.ToString(),

            // Arrays (simplified - return as string representation)
            var t when t.EndsWith("[]") => value.ToString(),

            // Default: keep as-is
            _ => value
        };
    }
    private void AddPerformanceStats(CdcSinkStatsAggregator stats)
    {
        
        _lastStats = stats;

        _lastCdcSinkStats.Enqueue(stats);

        while (_lastCdcSinkStats.Count > 25)
            _lastCdcSinkStats.TryDequeue(out _);
    }

    protected abstract Task<ICdcSinkConsumer> CreateConsumerAsync();
    protected abstract Task HandleInitialLoadAsync();

    public void Start()
    {
        if (_longRunningWork != null)
            return;

        if (Script.Disabled || Configuration.Disabled)
            return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);

        var threadName = $"{Tag} process: {Name}";
        _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
        {
            try
            {
                // This has lower priority than request processing, so we let the OS
                // schedule this appropriately
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                NativeMemory.EnsureRegistered();

                //TODO: egor conert this to async and avoid blocking on async code inside ?
                RunAsync().Wait();
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to run Cdc Sink {Name}", e);
            }
        }, null, ThreadNames.ForCdcSinkProcess(threadName, Tag, Name));

        if (Logger.IsInfoEnabled)
            Logger.Info($"Starting {Tag} process: '{Name}'.");

    }

    public static TestCdcSinkScriptResult TestScript(TestCdcSinkScript testScript, DocumentsOperationContext context, DocumentDatabase database)
    {
        testScript.Configuration.Initialize(connectionString: null);

        testScript.Configuration.TestMode = true;

        if (testScript.Configuration.Validate(out List<string> errors) == false)
        {
            throw new InvalidOperationException(
                $"Invalid Cdc Sink configuration for '{testScript.Configuration.Name}'. " +
                $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.");
        }

        if (testScript.Configuration.Scripts.Count != 1)
        {
            throw new InvalidOperationException(
                $"Invalid number of scripts. You have provided {testScript.Configuration.Scripts.Count} " +
                "while Cdc Sink test expects to get exactly 1 script");
        }

        if (string.IsNullOrEmpty(testScript.Message))
            throw new InvalidOperationException("Sample message in JSON format must be provided");

        using var messageDoc = context.Sync.ReadForMemory(new MemoryStream(Encoding.UTF8.GetBytes(testScript.Message)), "Cdc-sink-test-message");

        using (context.OpenWriteTransaction())
        {
            var script = new PatchRequest(testScript.Configuration.Scripts[0].Script, PatchRequestType.CdcSink);

            var command = new TestCdcMessageCommand(context, script, messageDoc);

            command.Execute(context, null);

            return new TestCdcSinkScriptResult
            {
                DebugOutput = command.DebugOutput,
                Actions = command.DebugActions
            };
        }
    }

    public void Stop(string reason)
    {
        if (_longRunningWork == null)
            return;

        string msg = $"Stopping {Tag} process: '{Name}'. Reason: {reason}";

        if (Logger.IsInfoEnabled)
        {
            Logger.Info(msg);
        }

        _cts.Cancel();

        var longRunningWork = _longRunningWork;
        _longRunningWork = null;

        if (longRunningWork != PoolOfThreads.LongRunningWork.Current)  // prevent a deadlock
            longRunningWork.Join(int.MaxValue);

        _consumer?.DisposeAsync().AsTask().Wait();
        _consumer = null;
    }

    private void HandleScriptParseException(Exception e)
    {
        var message = $"[{Name}] Could not parse script. Stopping Cdc Sink process.";

        if (Logger.IsInfoEnabled)
            Logger.Info(message, e);

        var key = $"{Tag}/{Name}";
        var details = new CdcSinkErrorsDetails();

        details.Errors.Enqueue(new CdcSinkErrorInfo(e.ToString()));

        var alert = AlertRaised.Create(
            Database.Name,
            Tag,
            message,
            AlertReason.CdcSink_ScriptError,
            NotificationSeverity.Error,
            key: key,
            details: details);

        Database.NotificationCenter.Add(alert);

        Stop(message);
    }

    private void EnterFallbackMode()
    {
        if (Statistics.LastConsumeErrorTime == null)
            FallbackTime = TimeSpan.FromSeconds(5);
        else
        {
           //  double the fallback time (but don't cross CdcSink.MaxFallbackTimeInSec)
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - Statistics.LastConsumeErrorTime.Value).TotalSeconds;

            FallbackTime = TimeSpan.FromSeconds(Math.Min(
                Database.Configuration.CdcSink
                    .MaxFallbackTime.AsTimeSpan.TotalSeconds,
                Math.Max(5, secondsSinceLastError * 2)));
        }
    }

    public CdcSinkPerformanceStats[] GetPerformanceStats()
    {
        var lastStats = _lastStats;

        return _lastCdcSinkStats
            .Select(x => x == lastStats ? x.ToPerformanceLiveStatsWithDetails() : x.ToPerformanceStats())
            .ToArray();


        return Array.Empty<CdcSinkPerformanceStats>();
    }

    public CdcSinkStatsAggregator GetLatestPerformanceStats()
    {
        return null;
        return _lastStats;
    }

    private bool CanContinueBatch(CdcSinkStatsScope stats, int batchSize, DocumentsOperationContext ctx)
    {
        if (Database.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            var reason = $"Stopping the batch after {stats.Duration} because the CPU credits balance is almost completely used";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);

            return false;
        }

        if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
        {
            var reason = $"The batch was stopped after processing {batchSize:#,#;;0} items because of low memory";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);
            return false;
        }

        var totalAllocated = new Size(_threadAllocations.TotalAllocated, SizeUnit.Bytes);
        _threadAllocations.CurrentlyAllocatedForProcessing = totalAllocated.GetValue(SizeUnit.Bytes);

        stats.RecordCurrentlyAllocated(totalAllocated.GetValue(SizeUnit.Bytes) + GC.GetAllocatedBytesForCurrentThread());

        if (totalAllocated > _currentMaximumAllowedMemory)
        {
            if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                    totalAllocated,
                    Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Database.ServerStore.Server.MetricCacher, Logger, out var memoryUsage) == false)
            {
                var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {totalAllocated}.";
                if (memoryUsage != null)
                {
                    reason += " Current memory usage: " +
                               $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                               $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordPullCompleteReason(reason);

                ctx.DoNotReuse = true;

                return false;
            }
        }

        var maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

        if (maxBatchSize != null && batchSize >= maxBatchSize)
        {
            var reason = $"Stopping the batch because maximum batch size limit was reached ({batchSize})";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordPullCompleteReason(reason);

            return false;
        }

        return true;
    }

    protected void EnsureThreadAllocationStats()
    {
        _threadAllocations = NativeMemory.CurrentThreadStats;
    }

    public void Dispose()
    {
        if (CancellationToken.IsCancellationRequested)
            return;

        var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

        exceptionAggregator.Execute(() => Stop("Dispose"));

        exceptionAggregator.Execute(() => _cts.Dispose());
        exceptionAggregator.Execute(() => _consumer?.DisposeAsync().AsTask().Wait()); // TODO: egor this is Disposed in Stop, no?

        exceptionAggregator.ThrowIfNeeded();
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        _lowMemoryFlag.Raise();
    }

    public void LowMemoryOver()
    {
        _lowMemoryFlag.Lower();
    }
}

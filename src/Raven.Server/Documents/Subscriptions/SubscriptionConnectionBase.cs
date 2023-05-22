﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Voron.Global;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionConnectionBase<TIncludesCommand> : ISubscriptionConnection
        where TIncludesCommand : AbstractIncludesCommand
    {
        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment ExceptionSegment = new StringSegment("Exception");

        public readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(1);

        protected readonly ServerStore _serverStore;
        private readonly IDisposable _tcpConnectionDisposable;
        internal readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;

        internal SubscriptionWorkerOptions _options;
        internal readonly Logger _logger;

        public readonly ConcurrentQueue<string> RecentSubscriptionStatuses = new ConcurrentQueue<string>();
        internal bool _isDisposed;
        public SubscriptionWorkerOptions Options => _options;
        public SubscriptionException ConnectionException;
        public long SubscriptionId { get; set; }
        public readonly string DatabaseName;
        public TcpConnectionOptions TcpConnection { get; }
        public CancellationTokenSource CancellationTokenSource { get; }

        public SubscriptionOpeningStrategy Strategy => _options.Strategy;
        public readonly string ClientUri;

        public string WorkerId => _options.WorkerId ??= Guid.NewGuid().ToString();
        public SubscriptionState SubscriptionState { get; private set; }
        public SubscriptionConnection.ParsedSubscription Subscription { get; private set; }

        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; }
        public readonly SubscriptionStatsCollector Stats;

        public Task SubscriptionConnectionTask { get; set; }
        private MemoryStream _buffer = new MemoryStream();

        protected AbstractSubscriptionProcessor<TIncludesCommand> Processor;

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal AsyncManualResetEvent PauseConnection;
        }

        protected SubscriptionConnectionBase(TcpConnectionOptions tcpConnection, ServerStore serverStore, JsonOperationContext.MemoryBuffer memoryBuffer,
            IDisposable tcpConnectionDisposable, string database, CancellationToken token)
        {
            TcpConnection = tcpConnection;
            _serverStore = serverStore;
            _copiedBuffer = memoryBuffer.Clone(serverStore.ContextPool);
            _tcpConnectionDisposable = tcpConnectionDisposable;

            DatabaseName = database;
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _logger = LoggingSource.Instance.GetLogger(ExtractDatabaseNameForLogging(tcpConnection), GetType().FullName);

            ClientUri = tcpConnection.TcpClient.Client.RemoteEndPoint.ToString();

            SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Subscription, tcpConnection.ProtocolVersion);
            Stats = new SubscriptionStatsCollector();
        }


        private static string ExtractDatabaseNameForLogging(TcpConnectionOptions tcpConnection)
        {
            if (tcpConnection.DocumentDatabase != null)
                return tcpConnection.DocumentDatabase.Name;

            if (tcpConnection.DatabaseContext != null)
                return tcpConnection.DatabaseContext.DatabaseName;

            return null;
        }

        public abstract AbstractSubscriptionProcessor<TIncludesCommand> CreateProcessor(SubscriptionConnectionBase<TIncludesCommand> connection);

        public async Task ProcessSubscriptionAsync<TState, TConnection>(TState state)
            where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludesCommand>
            where TConnection : SubscriptionConnectionBase<TIncludesCommand>
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to process subscription"));
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {ClientUri}");
            }

            using (Processor = CreateProcessor(this))
            {
                var replyFromClientTask = GetReplyFromClientAsync();

                AfterProcessorCreation();

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    _buffer.SetLength(0);

                    if (ForTestingPurposes?.PauseConnection != null)
                        await ForTestingPurposes.PauseConnection.WaitAsync();

                    var inProgressBatchStats = Stats.CreateInProgressBatchStats();

                    using (var batchScope = inProgressBatchStats.CreateScope())
                    {
                        try
                        {
                            using (MarkInUse())
                            {
                                var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                                var anyDocumentsSentInCurrentIteration =
                                    await TrySendingBatchToClient<TState, TConnection>(state, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);

                                if (anyDocumentsSentInCurrentIteration == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                                    }

                                    AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                                        $"Acknowledging docs processing progress without sending any documents to client."));

                                    Stats.UpdateBatchPerformanceStats(0, false);

                                    if (sendingCurrentBatchStopwatch.Elapsed > HeartbeatTimeout)
                                        await SendHeartBeatAsync("Didn't find any documents to send and more then 1000ms passed");

                                    if (FoundAboutMoreDocs())
                                        state.NotifyHasMoreDocs();

                                    AssertCloseWhenNoDocsLeft();

                                    if (await WaitForChangedDocsAsync(state, replyFromClientTask))
                                        continue;
                                }
                            }

                            using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                            {
                                replyFromClientTask = await WaitForClientAck(replyFromClientTask);
                            }

                            var last = Stats.UpdateBatchPerformanceStats(batchScope.GetBatchSize());
                            RaiseNotificationForBatchEnd(_options.SubscriptionName, last);
                        }
                        catch (Exception e)
                        {
                          //  Console.WriteLine("ProcessSubscriptionAsync: " + e);
                            OnError(e);
                            batchScope.RecordException(e.ToString());
                            throw;
                        }
                    }
                }

                CancellationTokenSource.Token.ThrowIfCancellationRequested();
            }
        }

        protected virtual void OnError(Exception e) { }

        protected List<(Document Document, Exception Exception)> CurrentBatch = new();

        /// <summary>
        /// Iterates on a batch in document collection, process it and send documents if found any match
        /// </summary>
        /// <returns>Whether succeeded finding any documents to send</returns>
        private async Task<bool> TrySendingBatchToClient<TState, TConnection>(TState state, Stopwatch sendingCurrentBatchStopwatch,
            SubscriptionBatchStatsScope batchScope, SubscriptionBatchStatsAggregator batchStatsAggregator)
            where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludesCommand>
            where TConnection : SubscriptionConnectionBase<TIncludesCommand>
        {
            if (await state.WaitForSubscriptionActiveLock(300) == false)
            {
                return false;
            }

            try
            {
                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start trying to send docs to client"));
                bool anyDocumentsSentInCurrentIteration = false;

                using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
                {
                    batchScope.RecordBatchInfo(state.SubscriptionId, state.SubscriptionName,
                        Stats.ConnectionStatsIdForConnection,
                        batchStatsAggregator.Id);

                    int docsToFlush = 0;
                    string lastChangeVectorSentInThisBatch = null;

                    using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, _buffer))
                    using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
                    using (clusterOperationContext.OpenReadTransaction())
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        using (Processor.InitializeForNewBatch(clusterOperationContext, out var includeCommand))
                        {
                            foreach (var result in Processor.GetBatch())
                            {
                                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                                lastChangeVectorSentInThisBatch = SetLastChangeVectorInThisBatch(clusterOperationContext, lastChangeVectorSentInThisBatch, result.Doc);

                                if (result.Doc.Data == null)
                                {
                                    if (sendingCurrentBatchStopwatch.Elapsed > HeartbeatTimeout)
                                    {
                                        await SendHeartBeatAsync("Skipping docs for more than 1000ms without sending any data");
                                        sendingCurrentBatchStopwatch.Restart();
                                    }

                                    continue;
                                }

                                anyDocumentsSentInCurrentIteration = true;

                                CurrentBatch.Add(result);

                                docsToFlush++;
                                batchScope.RecordDocumentInfo(result.Doc.Data.Size);

                                TcpConnection.LastEtagSent = result.Doc.Etag;
                            }

                            await UpdateStateAfterBatchSentAsync(clusterOperationContext, lastChangeVectorSentInThisBatch);

                            if (anyDocumentsSentInCurrentIteration)
                            {
                                if (CurrentBatch.Count == 0)
                                    return false;

                                foreach (var item in CurrentBatch)
                                {
                                    using (item.Document)
                                    {
                                        WriteDocument(writer, context, item, includeCommand);
                                    }
                                    //TODO: egor need to put it back?
                                    /*if (await FlushBatchIfNeededAsync(sendingCurrentBatchStopwatch, SubscriptionId, writer, _buffer, TcpConnection, Stats.Metrics, _logger, docsToFlush,
                                            CancellationTokenSource.Token))
                                    {
                                        sendingCurrentBatchStopwatch.Restart();
                                    }*/
                                }

                                if (includeCommand != null)
                                {
                                    await includeCommand.WriteIncludesAsync(writer, context, batchScope, CancellationTokenSource.Token);
                                }

                                WriteEndOfBatch(writer);

                                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Flushing sent docs to client"));

                                await FlushDocsToClientAsync(SubscriptionId, writer, _buffer, TcpConnection, Stats.Metrics, _logger, docsToFlush, endOfBatch: true, CancellationTokenSource.Token);
                            }
                        }
                    }
                }

                return anyDocumentsSentInCurrentIteration;
            }
            finally
            {
                CurrentBatch.Clear();
                state.ReleaseSubscriptionActiveLock();
            }
        }

        private void WriteDocument(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, (Document Doc, Exception Exception) result,
            TIncludesCommand includeCommand)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ISubscriptionConnection.TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(DataSegment));
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(DataSegment));
            result.Doc.EnsureMetadata();

            if (result.Exception != null)
            {
                if (result.Doc.Data.Modifications != null)
                {
                    result.Doc.Data = context.ReadObject(result.Doc.Data, "subsDocAfterModifications");
                }

                var metadata = result.Doc.Data[Client.Constants.Documents.Metadata.Key];
                writer.WriteValue(BlittableJsonToken.StartObject,
                    context.ReadObject(new DynamicJsonValue { [Client.Constants.Documents.Metadata.Key] = metadata }, result.Doc.Id)
                );
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ExceptionSegment));
                writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(result.Exception.ToString()));
            }
            else
            {
                GatherIncludesForDocument(includeCommand, result.Doc);
                writer.WriteDocument(context, result.Doc, metadataOnly: false);
            }

            writer.WriteEndObject();
        }

        protected abstract void GatherIncludesForDocument(TIncludesCommand includeDocuments, Document document);

        public async Task ReportExceptionAsync(SubscriptionError error, Exception e)
        {
            Stats.ConnectionScope.RecordException(error, e.ToString());
            if (ConnectionException == null && e is SubscriptionException se)
            {
                ConnectionException = se;
            }

            try
            {
                await LogExceptionAndReportToClientAsync(ConnectionException ?? e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        protected virtual async Task<bool> WaitForChangedDocsAsync(AbstractSubscriptionConnectionsState state, Task pendingReply)
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start waiting for changed documents"));
            do
            {
                var hasMoreDocsTask = state.WaitForMoreDocs();

                var resultingTask = await Task
                    .WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(HeartbeatTimeout)).ConfigureAwait(false);

                TcpConnection.DocumentDatabase?.ForTestingPurposes?.Subscription_ActionToCallDuringWaitForChangedDocuments?.Invoke();

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;

                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                    return true;

                await SendHeartBeatAsync("Waiting for changed documents");
                await SendNoopAckAsync();

                if (FoundAboutMoreDocs())
                    return true;

            } while (CancellationTokenSource.IsCancellationRequested == false);

            return false;
        }

        internal async Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync()
        {
            try
            {
                using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var blittable = await context.ParseToMemoryAsync(
                           TcpConnection.Stream,
                           "Reply from subscription client",
                           BlittableJsonDocumentBuilder.UsageMode.None,
                           _copiedBuffer.Buffer))
                {
                    TcpConnection.LastEtagReceived = -1;
                    TcpConnection.RegisterBytesReceived(blittable.Size);
                    return JsonDeserializationServer.SubscriptionConnectionClientMessage(blittable);
                }
            }
            catch (EndOfStreamException e)
            {
                throw new SubscriptionConnectionDownException("No reply from the subscription client.", e);
            }
            catch (IOException)
            {
                if (_isDisposed == false)
                    throw;

                return new SubscriptionConnectionClientMessage { ChangeVector = null, Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification };
            }
            catch (ObjectDisposedException)
            {
                return new SubscriptionConnectionClientMessage { ChangeVector = null, Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification };
            }
        }

        public async Task InitAsync()
        {
            var message = CreateStatusMessage(ConnectionStatus.Create);
            AddToStatusDescription(message);
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message);
            }

            // first, validate details and make sure subscription exists
            await RefreshAsync();

            AssertSupportedFeatures();
        }

        public async Task RefreshAsync(long? registerConnectionDurationInTicks = null)
        {
            SubscriptionState = await AssertSubscriptionConnectionDetails(registerConnectionDurationInTicks);
            Subscription = SubscriptionConnection.ParseSubscriptionQuery(SubscriptionState.Query);
        }

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long? registerConnectionDurationInTicks) =>
            await AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, registerConnectionDurationInTicks, CancellationTokenSource.Token);

        protected virtual RawDatabaseRecord GetRecord(ClusterOperationContext context) => _serverStore.Cluster.ReadRawDatabaseRecord(context, DatabaseName);

        protected abstract string WhosTaskIsIt(DatabaseTopology topology, SubscriptionState subscriptionState);

        private async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name, long? registerConnectionDurationInTicks, CancellationToken token)
        {
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id, token);

            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var record = GetRecord(context))
            {
#pragma warning disable CS0618
                var subscription = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, DatabaseName, name);
#pragma warning restore CS0618
                var topology = record.TopologyForSubscriptions();

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19089 create subscription WhosTaskIsIt");
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19089 Need to handle NodeTag, currently is isn't used for sharded because it is shared");

                var whoseTaskIsIt = WhosTaskIsIt(topology, subscription);

                if (whoseTaskIsIt == null && record.DeletionInProgress.ContainsKey(_serverStore.NodeTag))
                    throw new DatabaseDoesNotExistException(
                        $"Stopping subscription '{name}' on node {_serverStore.NodeTag}, because database '{DatabaseName}' is being deleted.");

                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    var databaseTopologyAvailabilityExplanation = new Dictionary<string, string>();

                    string generalState;
                    RachisState currentState = _serverStore.Engine.CurrentState;
                    if (currentState == RachisState.Candidate || currentState == RachisState.Passive)
                    {
                        generalState =
                            $"Current node ({_serverStore.NodeTag}) is in {currentState.ToString()} state therefore, we can't answer who's task is it and returning null";
                    }
                    else
                    {
                        generalState = currentState.ToString();
                    }

                    databaseTopologyAvailabilityExplanation["NodeState"] = generalState;

                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Rehabs, stateName: "rehab");
                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Promotables,
                        stateName: "promotable");

                    foreach (var member in topology.Members)
                    {
                        if (whoseTaskIsIt != null)
                        {
                            if (whoseTaskIsIt == subscription.MentorNode && member == subscription.MentorNode)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is the mentor node and a valid member of the topology, it should be the mentor node";
                            }
                            else if (whoseTaskIsIt != null && whoseTaskIsIt != member)
                            {
                                databaseTopologyAvailabilityExplanation[member] =
                                    "Is a valid member of the topology, but not chosen to be the node running the subscription";
                            }
                            else if (whoseTaskIsIt == member)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is a valid member of the topology and is chosen to be running the subscription";
                            }
                        }
                        else
                        {
                            databaseTopologyAvailabilityExplanation[member] =
                                "Is a valid member of the topology but was not chosen to run the subscription, we didn't find any other match either";
                        }
                    }

                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription with id '{id}' and name '{name}' can't be processed on current node ({_serverStore.NodeTag}), because it belongs to {whoseTaskIsIt}",
                        whoseTaskIsIt,
                        databaseTopologyAvailabilityExplanation, id)
                    { RegisterConnectionDurationInTicks = registerConnectionDurationInTicks };
                }

                if (subscription.Disabled)
                    throw new SubscriptionClosedException($"The subscription with id '{id}' and name '{name}' is disabled and cannot be used until enabled");

                return subscription;
            }

            static void FillNodesAvailabilityReportForState(SubscriptionState subscription, DatabaseTopology topology,
                Dictionary<string, string> databaseTopologyAvailabilityExplenation, List<string> stateGroup, string stateName)
            {
                foreach (var nodeInGroup in stateGroup)
                {
                    var rehabMessage = string.Empty;
                    if (subscription.MentorNode == nodeInGroup)
                    {
                        rehabMessage = $"Although this node is a mentor, it's state is {stateName} and can't run the subscription";
                    }
                    else
                    {
                        rehabMessage = $"Node's state is {stateName}, can't run subscription";
                    }

                    if (topology.DemotionReasons.TryGetValue(nodeInGroup, out var demotionReason))
                    {
                        rehabMessage = rehabMessage + ". Reason:" + demotionReason;
                    }

                    databaseTopologyAvailabilityExplenation[nodeInGroup] = rehabMessage;
                }
            }
        }

        public void RecordConnectionInfo() => Stats.ConnectionScope.RecordConnectionInfo(SubscriptionState, ClientUri, Options.Strategy, WorkerId);

        protected virtual void AssertSupportedFeatures()
        {
            if (_options.Strategy == SubscriptionOpeningStrategy.Concurrent)
            {
                _serverStore.LicenseManager.AssertCanAddConcurrentDataSubscriptions();
            }

            if (SupportedFeatures.Subscription.Includes == false)
            {
                if (Subscription.Includes != null && Subscription.Includes.Length > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Includes.");
            }

            if (SupportedFeatures.Subscription.CounterIncludes == false)
            {
                if (Subscription.CounterIncludes != null && Subscription.CounterIncludes.Length > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Counter Includes.");
            }

            if (SupportedFeatures.Subscription.TimeSeriesIncludes == false)
            {
                if (Subscription.TimeSeriesIncludes != null && Subscription.TimeSeriesIncludes.TimeSeries.Count > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support TimeSeries Includes.");
            }
        }

        protected virtual void AssertCloseWhenNoDocsLeft()
        {
            if (_options.CloseWhenNoDocsLeft)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Closing subscription {Options.SubscriptionName} because did not find any documents to send and it's in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode");
                }

                throw new SubscriptionClosedException(
                    $"Closing subscription {Options.SubscriptionName} because there were no documents left and client connected in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode", canReconnect: false, noDocsLeft: true);
            }
        }

        protected async Task LogExceptionAndReportToClientAsync(Exception e)
        {
            var errorMessage = CreateStatusMessage(ConnectionStatus.Fail, e.ToString());
            AddToStatusDescription($"{errorMessage}. Sending response to client");
            if (_logger.IsInfoEnabled && e is not OperationCanceledException)
                _logger.Info(errorMessage, e);

            await ReportExceptionToClientAsync(e);
        }

        protected async Task ReportExceptionToClientAsync(Exception ex, int recursionDepth = 0)
        {
            if (recursionDepth == 2)
                return;
            try
            {
                switch (ex)
                {
                    case SubscriptionDoesNotExistException:
                    case DatabaseDoesNotExistException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.NotFound),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionClosedException sce:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Closed),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                            [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                            {
                                [nameof(SubscriptionClosedException.CanReconnect)] = sce.CanReconnect,
                                [nameof(SubscriptionClosedException.NoDocsLeft)] = sce.NoDocsLeft
                            }
                        });
                        break;
                    case SubscriptionInvalidStateException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionInUseException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.InUse),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionDoesNotBelongToNodeException subscriptionDoesNotBelongException:
                        {
                            if (string.IsNullOrEmpty(subscriptionDoesNotBelongException.AppropriateNode) == false)
                            {
                                try
                                {
                                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                                    using (ctx.OpenReadTransaction())
                                    {
                                        // check that the subscription exists on AppropriateNode
                                        var clusterTopology = _serverStore.GetClusterTopology(ctx);
                                        using (var requester = ClusterRequestExecutor.CreateForSingleNode(
                                                   clusterTopology.GetUrlFromTag(subscriptionDoesNotBelongException.AppropriateNode),
                                                   _serverStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                                        {
                                            await requester.ExecuteAsync(new WaitForRaftIndexCommand(subscriptionDoesNotBelongException.Index), ctx);
                                        }
                                    }
                                }
                                catch
                                {
                                    // we let the client try to connect to AppropriateNode
                                }
                            }

                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Redirecting subscription client to different server"));
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription does not belong to current node", ex);
                            }

                            await WriteJsonAsync(new DynamicJsonValue
                            {
                                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Redirect),
                                [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                                [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                                [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                                {
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)] = subscriptionDoesNotBelongException.AppropriateNode,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.CurrentTag)] = _serverStore.NodeTag,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RegisterConnectionDurationInTicks)] =
                                        subscriptionDoesNotBelongException.RegisterConnectionDurationInTicks,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)] =
                                        new DynamicJsonArray(subscriptionDoesNotBelongException.Reasons.Select(item => new DynamicJsonValue { [item.Key] = item.Value }))
                                }
                            });
                            break;
                        }
                    case SubscriptionChangeVectorUpdateConcurrencyException:
                        {
                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                                $"Subscription change vector update concurrency error, reporting to '{ClientUri}'"));
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription change vector update concurrency error", ex);
                            }

                            await WriteJsonAsync(new DynamicJsonValue
                            {
                                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect),
                                [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                                [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                            });
                            break;
                        }
                    case LicenseLimitException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case RachisApplyException commandExecution when commandExecution.InnerException is SubscriptionException:
                        await ReportExceptionToClientAsync(commandExecution.InnerException, recursionDepth - 1);
                        break;
                    default:

                        if (ex is not OperationCanceledException)
                        {
                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Fail, $"Subscription error on subscription {ex}"));

                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription error", ex);
                            }
                        }

                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Error),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.None),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                }
            }
            catch
            {
                // ignored
            }
        }

        public enum ConnectionStatus
        {
            Create,
            Fail,
            Info
        }

        public class StatusMessageDetails
        {
            public string DatabaseName;
            public string ClientType;
            public string SubscriptionType;
        }

        protected string CreateStatusMessage(ConnectionStatus status, string info = null)
        {
            var message = GetStatusMessageDetails();
            var dbNameStr = message.DatabaseName;
            var clientType = message.ClientType;
            var subsType = message.SubscriptionType;

            string m = null;
            switch (status)
            {
                case ConnectionStatus.Create:
                    m = $"[CREATE] Received a connection for {subsType}, {dbNameStr} from {clientType}";
                    break;
                case ConnectionStatus.Fail:
                    m = $"[FAIL] for {subsType}, {dbNameStr} from {clientType}";
                    break;
                case ConnectionStatus.Info:
                    return $"[INFO] Update for {subsType}, {dbNameStr}, with {clientType}: {info}";
                default:
                    throw new ArgumentOutOfRangeException(nameof(status), status, null);
            }

            if (info == null)
                return m;
            return $"{m}, {info}";
        }

        public virtual void FinishProcessing()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing subscription"));

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Finished processing subscription {SubscriptionId} / from client {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }
        }

        public void AddToStatusDescription(string message)
        {
            while (RecentSubscriptionStatuses.Count > 50)
            {
                RecentSubscriptionStatuses.TryDequeue(out _);
            }
            // Console.WriteLine(message);
            RecentSubscriptionStatuses.Enqueue(message);
        }

        public virtual async Task ParseSubscriptionOptionsAsync()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (BlittableJsonReaderObject subscriptionCommandOptions = await context.ParseToMemoryAsync(
                       TcpConnection.Stream,
                       "subscription options",
                       BlittableJsonDocumentBuilder.UsageMode.None,
                       _copiedBuffer.Buffer,
                       token: CancellationTokenSource.Token))
            {
                _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);

                if (string.IsNullOrEmpty(_options.SubscriptionName))
                    return;

                context.OpenReadTransaction();

                var subscriptionItemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, _options.SubscriptionName);
                var translation = _serverStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionDoesNotExistException("Cannot find any Subscription Task with name: " + _options.SubscriptionName);

                if (translation.TryGet(nameof(Client.Documents.Subscriptions.SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the Subscription Task ID for subscription named: " + _options.SubscriptionName);

                SubscriptionId = id;
            }
        }

        private async Task WriteJsonAsync(DynamicJsonValue value)
        {
            int writtenBytes;
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, TcpConnection.Stream))
            {
                context.Write(writer, value);
                writtenBytes = writer.Position;
            }

            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(writtenBytes);
        }

        public Task SendAcceptMessageAsync() => WriteJsonAsync(AcceptMessage());

        protected virtual DynamicJsonValue AcceptMessage()
        {
            return new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            };
        }

        internal async Task SendHeartBeatAsync(string reason)
        {
            try
            {
                await TcpConnection.Stream.WriteAsync(Heartbeat, 0, Heartbeat.Length, CancellationTokenSource.Token);
                await TcpConnection.Stream.FlushAsync();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Subscription {Options.SubscriptionName} is sending a Heartbeat message to the client. Reason: {reason}");
                }
            }
            catch (Exception ex)
            {
                throw new SubscriptionClosedException($"Cannot contact client anymore, closing subscription ({Options?.SubscriptionName})",
                    canReconnect: ex is OperationCanceledException, ex);
            }

            TcpConnection.RegisterBytesSent(Heartbeat.Length);
        }

        private async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask)
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Waiting for acknowledge from client."));

            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                var result = await Task.WhenAny(replyFromClientTask, TimeoutManager.WaitFor(HeartbeatTimeout, CancellationTokenSource.Token));
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (result == replyFromClientTask)
                {
                    clientReply = await replyFromClientTask;
                    if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                    {
                        CancellationTokenSource.Cancel();
                        break;
                    }

                    replyFromClientTask = GetReplyFromClientAsync();
                    break;
                }

                await SendHeartBeatAsync("Waiting for client ACK");
                await SendNoopAckAsync();
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    {
                        AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Got acknowledge from client."));
                        await OnClientAckAsync(clientReply.ChangeVector);
                        break;
                    }
                //precaution, should not reach this case...
                case SubscriptionConnectionClientMessage.MessageType.DisposedNotification:
                    CancellationTokenSource.Cancel();
                    break;

                default:
                    throw new ArgumentException("Unknown message type from client " +
                                                clientReply.Type);
            }

            return replyFromClientTask;
        }

        protected async Task SendConfirmAsync(DateTime time)
        {
            Stats.Metrics.LastAckReceivedAt = time;
            Stats.Metrics.AckRate?.Mark();
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
            });
        }

        protected abstract StatusMessageDetails GetStatusMessageDetails();

        protected abstract Task OnClientAckAsync(string clientReplyChangeVector);
        public abstract Task SendNoopAckAsync();
        protected abstract bool FoundAboutMoreDocs();
        public abstract IDisposable MarkInUse();

        protected abstract void AfterProcessorCreation();
        protected abstract void RaiseNotificationForBatchEnd(string name, SubscriptionBatchStatsAggregator last);
        protected abstract string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, Document sentDocument);
        protected abstract Task UpdateStateAfterBatchSentAsync(IChangeVectorOperationContext context, string lastChangeVectorSentInThisBatch);

        internal static void WriteEndOfBatch(AsyncBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
            writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
            writer.WriteEndObject();
        }

        internal static async Task<bool> FlushBatchIfNeededAsync(Stopwatch sendingCurrentBatchStopwatch, long subscriptionId, AsyncBlittableJsonTextWriter writer,
            MemoryStream buffer, TcpConnectionOptions tcpConnection, SubscriptionConnectionMetrics metrics, Logger logger, int docsToFlush,
            CancellationToken token = default)
        {
            // perform flush for current batch after 1000ms of running or 1 MB
            if (buffer.Length < Constants.Size.Megabyte && sendingCurrentBatchStopwatch.ElapsedMilliseconds < 1000)
                return false;

            await FlushDocsToClientAsync(subscriptionId, writer, buffer, tcpConnection, metrics, logger, docsToFlush, endOfBatch: false, token: token);
            if (logger != null && logger.IsInfoEnabled)
            {
                logger.Info($"Finished flushing a batch with {docsToFlush} documents for subscription {subscriptionId}");
            }

            return true;
        }

        internal static async Task FlushDocsToClientAsync(long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer,
            TcpConnectionOptions tcpConnection, SubscriptionConnectionMetrics metrics, Logger logger, int flushedDocs, bool endOfBatch = false,
            CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (logger != null && logger.IsInfoEnabled)
                logger.Info(
                    $"Flushing {flushedDocs} documents for subscription {subscriptionId} sending to {tcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch ? ", ending batch" : string.Empty)}");

            await writer.FlushAsync(token);
            var bufferSize = buffer.Length;
            await FlushBufferToNetworkAsync(buffer, tcpConnection, token);

            tcpConnection.RegisterBytesSent(bufferSize);

            if (metrics != null)
            {
                metrics.LastMessageSentAt = DateTime.UtcNow;
                metrics.DocsRate?.Mark(flushedDocs);
                metrics.BytesRate?.Mark(bufferSize);
            }
        }

        internal static async Task FlushBufferToNetworkAsync(MemoryStream buffer, TcpConnectionOptions tcpConnection, CancellationToken token = default)
        {
            buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await tcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count, token);
            await tcpConnection.Stream.FlushAsync(token);
            tcpConnection.RegisterBytesSent(bytes.Count);
            buffer.SetLength(0);
        }

        public virtual void Dispose()
        {
            using (_copiedBuffer.ReleaseBuffer)
            {
                try
                {
                    CancellationTokenSource.Cancel();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    _tcpConnectionDisposable?.Dispose();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    TcpConnection.Dispose();
                }
                catch
                {
                    // ignored
                }


                try
                {
                    CancellationTokenSource.Dispose();
                }
                catch
                {
                    // ignored
                }

                RecentSubscriptionStatuses?.Clear();
            }

            Stats?.Dispose();
        }
    }
}

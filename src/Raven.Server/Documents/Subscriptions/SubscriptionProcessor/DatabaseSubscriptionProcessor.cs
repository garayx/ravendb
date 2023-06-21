﻿
using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public abstract class DatabaseSubscriptionProcessor<T> : DatabaseSubscriptionProcessor
    {
        protected SubscriptionFetcher<T> Fetcher;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out DatabaseIncludesCommandImpl includesCommands)
        {
            var release = base.InitializeForNewBatch(clusterContext, out includesCommands);

            try
            {
                Fetcher = CreateFetcher();
                Fetcher.Initialize(clusterContext, DocsContext, Active);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected (Document Doc, Exception Exception, bool IsActiveMigration) GetBatchItem(T item)
        {
            if (ShouldSend(item, out var reason, out var exception, out var result, out var isActiveMigration))
            {
                if (IncludesCmd != null && IncludesCmd.IncludeDocumentsCommand != null && Run != null)
                    IncludesCmd.IncludeDocumentsCommand.AddRange(Run.Includes, result.Id);

                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, null, false);
            }

            if (Logger.IsInfoEnabled)
                Logger.Info(reason, exception);

            if (exception != null)
            {
                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, exception, false);
            }

            result.Data = null;
            return (result, null, isActiveMigration);
        }

        protected abstract SubscriptionFetcher<T> CreateFetcher();

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception, out Document result, out bool isActiveMigration);
    }

    public abstract class DatabaseSubscriptionProcessor : AbstractSubscriptionProcessor<DatabaseIncludesCommandImpl>
    {
        protected readonly Size MaximumAllowedMemory;

        protected readonly DocumentDatabase Database;
        protected DocumentsOperationContext DocsContext;
        protected SubscriptionConnectionsState SubscriptionConnectionsState;
        protected HashSet<long> Active;

        public SubscriptionPatchDocument Patch;
        protected ScriptRunner.SingleRun Run;
        private ScriptRunner.ReturnRun? _returnRun;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) : base(server, connection, database.Name)
        {
            Database = database;
            MaximumAllowedMemory = new Size(Database.Is32Bits ? 4 : 32, SizeUnit.Megabytes);
        }

        public override void InitializeProcessor()
        {
            base.InitializeProcessor();

            SubscriptionConnectionsState = Database.SubscriptionStorage.Subscriptions[Connection.SubscriptionId];
            Active = SubscriptionConnectionsState.GetActiveBatches();
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out DatabaseIncludesCommandImpl includesCommands)
        {
            var release = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocsContext);
            try
            {
                DocsContext.OpenReadTransaction();
                base.InitializeForNewBatch(clusterContext, out includesCommands);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected override DatabaseIncludesCommandImpl CreateIncludeCommands()
        {
            var includes = CreateIncludeCommandsInternal(Database, DocsContext, Connection, Connection.Subscription);
            return includes;
        }

        internal static DatabaseIncludesCommandImpl CreateIncludeCommandsInternal(DocumentDatabase database, DocumentsOperationContext context,
            ISubscriptionConnection connection, SubscriptionConnection.ParsedSubscription subscription)
        {
            var hasIncludes = TryCreateIncludesCommand(database, context, connection, subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries);

            var includes = hasIncludes ? new DatabaseIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters) : null;
            return includes;
        }

        public static bool TryCreateIncludesCommand(DocumentDatabase database, DocumentsOperationContext context, ISubscriptionConnection connection,
            SubscriptionConnection.ParsedSubscription subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries)
        {
            includeTimeSeries = null;
            includeCounters = null;
            includeDocuments = null;

            bool hasIncludes = false;
            if (connection == null)
            {
                if (subscription.Includes != null)
                {
                    // test subscription with includes
                    includeDocuments = new IncludeDocumentsCommand(database.DocumentsStorage, context, subscription.Includes,
                        isProjection: string.IsNullOrWhiteSpace(subscription.Script) == false);
                    hasIncludes = true;
                }
            }
            else if (connection.SupportedFeatures.Subscription.Includes)
            {
                includeDocuments = new IncludeDocumentsCommand(database.DocumentsStorage, context, subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(subscription.Script) == false);
                hasIncludes = true;
            }

            if (subscription.CounterIncludes != null)
            {
                if (connection != null && connection.SupportedFeatures.Subscription.CounterIncludes)
                {
                    includeCounters = new IncludeCountersCommand(database, context, subscription.CounterIncludes);
                    hasIncludes = true;
                }
                else
                {
                    includeCounters = new IncludeCountersCommand(database, context, subscription.CounterIncludes);
                    hasIncludes = true;
                }
            }

            if (subscription.TimeSeriesIncludes != null)
            {
                if (connection != null && connection.SupportedFeatures.Subscription.TimeSeriesIncludes)
                {
                    includeTimeSeries = new IncludeTimeSeriesCommand(context, subscription.TimeSeriesIncludes.TimeSeries);
                    hasIncludes = true;
                }
                else
                {
                    includeTimeSeries = new IncludeTimeSeriesCommand(context, subscription.TimeSeriesIncludes.TimeSeries);
                    hasIncludes = true;
                }
            }

            return hasIncludes;
        }

        protected void InitializeScript()
        {
            if (Patch == null)
                return;

            if (_returnRun != null)
                return; // already init

            _returnRun = Database.Scripts.GetScriptRunner(Patch, true, out Run);
        }

        private protected class ProjectionMetadataModifier : JsBlittableBridge.IResultModifier
        {
            public static readonly ProjectionMetadataModifier Instance = new ProjectionMetadataModifier();

            private ProjectionMetadataModifier()
            {
            }

            public void Modify(ObjectInstance json)
            {
                ObjectInstance metadata;
                var value = json.Get(Constants.Documents.Metadata.Key);
                if (value.Type == Types.Object)
                    metadata = value.AsObject();
                else
                {
                    metadata = new JsObject(json.Engine);
                    json.Set(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Set(Constants.Documents.Metadata.Projection, JsBoolean.True, false);
            }
        }

        public abstract long GetLastItemEtag(DocumentsOperationContext context, string collection);

        public override void Dispose()
        {
            base.Dispose();
            _returnRun?.Dispose();
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Properties;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Constants = Raven.Client.Constants;
using Size = Sparrow.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamSource : ISmugglerSource, IDisposable
    {
        private readonly PeepingTomStream _peepingTomStream;
        private readonly DocumentsOperationContext _context;
        private readonly DocumentDatabase _database;
        private readonly Logger _log;

        private JsonOperationContext.ManagedPinnedBuffer _buffer;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private JsonOperationContext.ManagedPinnedBuffer _writeBuffer;
        private JsonOperationContext.ReturnBuffer _returnWriteBuffer;
        private JsonParserState _state;
        private UnmanagedJsonParser _parser;
        private DatabaseItemType? _currentType;

        private SmugglerResult _result;

        private BuildVersionType _buildVersionType;
        private bool _readLegacyEtag;

        private Size _totalObjectsRead = new Size(0, SizeUnit.Bytes);
        private DatabaseItemType _operateOnTypes;

        public StreamSource(Stream stream, DocumentsOperationContext context, DocumentDatabase database)
        {
            _peepingTomStream = new PeepingTomStream(stream, context);
            _context = context;
            _database = database;
            _log = LoggingSource.Instance.GetLogger<StreamSource>(database.Name);
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion)
        {
            _result = result;
            _returnBuffer = _context.GetManagedBuffer(out _buffer);
            _state = new JsonParserState();
            _parser = new UnmanagedJsonParser(_context, _state, "file");

            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json.", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start object, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            _operateOnTypes = options.OperateOnTypes;
            buildVersion = ReadBuildVersion();
            _buildVersionType = BuildVersion.Type(buildVersion);
#pragma warning disable 618
            _readLegacyEtag = options.ReadLegacyEtag;
#pragma warning restore 618

            return new DisposableAction(() =>
            {
                _parser.Dispose();
                _returnBuffer.Dispose();
                _returnWriteBuffer.Dispose();
            });
        }

        public DatabaseItemType GetNextType()
        {
            if (_currentType != null)
            {
                var currentType = _currentType.Value;
                _currentType = null;

                return currentType;
            }

            var type = ReadType();
            var dbItemType = GetType(type);
            while (dbItemType == DatabaseItemType.Unknown)
            {
                var msg = $"You are trying to import items of type '{type}' which is unknown or not supported in {RavenVersionAttribute.Instance.Version}. Ignoring items.";
                if (_log.IsOperationsEnabled)
                    _log.Operations(msg);
                _result.AddWarning(msg);

                SkipArray(onSkipped: null, MaySkipBlob, CancellationToken.None);
                type = ReadType();
                dbItemType = GetType(type);
            }

            return dbItemType;
        }

        public DatabaseRecord GetDatabaseRecord()
        {
            var databaseRecord = new DatabaseRecord();
            ReadObject(reader =>
            {
                if (reader.TryGet(nameof(databaseRecord.Revisions), out BlittableJsonReaderObject revisions) &&
                    revisions != null)
                {
                    try
                    {
                        databaseRecord.Revisions = JsonDeserializationCluster.RevisionsConfiguration(revisions);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the revisions configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Expiration), out BlittableJsonReaderObject expiration) &&
                    expiration != null)
                {
                    try
                    {
                        databaseRecord.Expiration = JsonDeserializationCluster.ExpirationConfiguration(expiration);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the expiration configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.ConflictSolverConfig), out BlittableJsonReaderObject conflictSolverConfig) &&
                    conflictSolverConfig != null)
                {
                    try
                    {
                        databaseRecord.ConflictSolverConfig = JsonDeserializationCluster.ConflictSolverConfig(conflictSolverConfig);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the Conflict Solver Config configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.PeriodicBackups), out BlittableJsonReaderArray periodicBackups) &&
                    periodicBackups != null)
                {
                    databaseRecord.PeriodicBackups = new List<PeriodicBackupConfiguration>();
                    foreach (BlittableJsonReaderObject backup in periodicBackups)
                    {
                        try
                        {
                            var periodicBackup = JsonDeserializationCluster.PeriodicBackupConfiguration(backup);
                            databaseRecord.PeriodicBackups.Add(periodicBackup);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the periodic Backup configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Settings), out BlittableJsonReaderArray settings) &&
                    settings != null)
                {
                    databaseRecord.Settings = new Dictionary<string, string>();
                    foreach (BlittableJsonReaderObject config in settings)
                    {
                        try
                        {
                            var key = config.GetPropertyNames()[0];
                            config.TryGet(key, out string val);
                            databaseRecord.Settings[key] = val;
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the settings configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.UnusedDatabaseIds), out BlittableJsonReaderArray unusedDatabaseIds) &&
                    unusedDatabaseIds != null)
                {
                    foreach (var id in unusedDatabaseIds)
                    {
                        if(id is LazyStringValue == false && id is LazyCompressedStringValue == false)
                            throw new InvalidOperationException($"{nameof(databaseRecord.UnusedDatabaseIds)} should be a collection of strings but got {id.GetType()}");
                        databaseRecord.UnusedDatabaseIds.Add(id.ToString());
                    }
                }
                
                if (reader.TryGet(nameof(databaseRecord.ExternalReplications), out BlittableJsonReaderArray externalReplications) &&
                    externalReplications != null)
                {
                    databaseRecord.ExternalReplications = new List<ExternalReplication>();
                    foreach (BlittableJsonReaderObject replication in externalReplications)
                    {
                        try
                        {
                            databaseRecord.ExternalReplications.Add(JsonDeserializationCluster.ExternalReplication(replication));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the External Replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Sorters), out BlittableJsonReaderObject sorters) &&
                    sorters != null)
                {
                    databaseRecord.Sorters = new Dictionary<string, SorterDefinition>();

                    try
                    {
                        foreach (var sorterName in sorters.GetPropertyNames())
                        {
                            if (sorters.TryGet(sorterName, out BlittableJsonReaderObject sorter) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the sorters {sorterName} from smuggler file. Skipping.");

                                continue;
                            }

                            databaseRecord.Sorters[sorterName] = JsonDeserializationCluster.Sorters(sorter);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the sorters configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SinkPullReplications), out BlittableJsonReaderArray sinkPullReplications) &&
                    sinkPullReplications != null)
                {
                    databaseRecord.SinkPullReplications = new List<PullReplicationAsSink>();
                    foreach (BlittableJsonReaderObject pullReplication in sinkPullReplications)
                    {
                        try
                        {
                            var sink = JsonDeserializationCluster.PullReplicationAsSink(pullReplication);
                            databaseRecord.SinkPullReplications.Add(sink);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import sink pull replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.HubPullReplications), out BlittableJsonReaderArray hubPullReplications) &&
                    hubPullReplications != null)
                {
                    databaseRecord.HubPullReplications = new List<PullReplicationDefinition>();
                    foreach (BlittableJsonReaderObject pullReplication in hubPullReplications)
                    {
                        try
                        {
                            var hub = JsonDeserializationCluster.PullReplicationDefinition(pullReplication);
                            databaseRecord.HubPullReplications.Add(hub);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info($"Wasn't able to import the pull replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.RavenEtls), out BlittableJsonReaderArray ravenEtls) &&
                    ravenEtls != null)
                {
                    databaseRecord.RavenEtls = new List<RavenEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in ravenEtls)
                    {
                        try
                        {
                            databaseRecord.RavenEtls.Add(JsonDeserializationCluster.RavenEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Raven Etls configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SqlEtls), out BlittableJsonReaderArray sqlEtls) &&
                    sqlEtls != null)
                {
                    databaseRecord.SqlEtls = new List<SqlEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in sqlEtls)
                    {
                        try
                        {
                            databaseRecord.SqlEtls.Add(JsonDeserializationCluster.SqlEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Raven SQL Etls configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject ravenConnectionStrings) &&
                    ravenConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in ravenConnectionStrings.GetPropertyNames())
                        {
                            if (ravenConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the RavenDB connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.RavenConnectionString(connection);
                            databaseRecord.RavenConnectionStrings[connectionName] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.RavenConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the RavenDB connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SqlConnectionStrings), out BlittableJsonReaderObject sqlConnectionStrings) &&
                    sqlConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in sqlConnectionStrings.GetPropertyNames())
                        {
                            if (sqlConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the SQL connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.SqlConnectionString(connection);
                            databaseRecord.SqlConnectionStrings[connectionString.Name] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.SqlConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the SQL connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Client), out BlittableJsonReaderObject client) &&
                    client != null)
                {
                    try
                    {
                        databaseRecord.Client = JsonDeserializationCluster.ClientConfiguration(client);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the client configuration from smuggler file. Skipping.", e);
                    }
                }
            });

            return databaseRecord;
        }

        public IEnumerable<(string key, long index, BlittableJsonReaderObject value)> GetCompareExchangeValues(INewCompareExchangeActions actions)
        {
            return InternalGetCompareExchangeValues(actions);
        }

        public IEnumerable<string> GetCompareExchangeTombstones()
        {
            return InternalGetCompareExchangeTombstones();
        }

        public IEnumerable<CounterGroupDetail> GetCounterValues(List<string> collectionsToExport, ICounterActions actions)
        {
            return InternalGetCounterValues(actions);
        }

        public IEnumerable<CounterDetail> GetLegacyCounterValues()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    if (reader.TryGet(nameof(CounterItem.DocId), out string docId) == false ||
                        reader.TryGet(nameof(CounterItem.ChangeVector), out string cv) == false ||
                        reader.TryGet(nameof(CounterItem.Legacy.Name), out string name) == false ||
                        reader.TryGet(nameof(CounterItem.Legacy.Value), out long value) == false)
                    {
                        _result.Counters.ErroredCount++;
                        _result.AddWarning("Could not read counter entry.");
                        continue;
                    }

                    yield return new CounterDetail
                    {
                        DocumentId = docId,
                        ChangeVector = cv,
                        CounterName = name,
                        TotalValue = value
                    };
                }
            }
        }

        public IEnumerable<SubscriptionState> GetSubscriptions()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    if (reader.TryGet(nameof(SubscriptionState.SubscriptionName), out string subscriptionName) == false ||
                        reader.TryGet(nameof(SubscriptionState.Query), out string query) == false ||
                        reader.TryGet(nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint), out string changeVectorForNextBatchStartingPoint) == false ||
                        reader.TryGet(nameof(SubscriptionState.MentorNode), out string mentorNode) == false ||
                        reader.TryGet(nameof(SubscriptionState.NodeTag), out string nodeTag) == false ||
                        reader.TryGet(nameof(SubscriptionState.LastBatchAckTime), out DateTime lastBatchAckTime) == false ||
                        reader.TryGet(nameof(SubscriptionState.LastClientConnectionTime), out DateTime lastClientConnectionTime) == false ||
                        reader.TryGet(nameof(SubscriptionState.Disabled), out bool disabled) == false ||
                        reader.TryGet(nameof(SubscriptionState.SubscriptionId), out long subscriptionId) == false)
                    {
                        _result.Subscriptions.ErroredCount++;
                        _result.AddWarning("Could not read subscriptions entry.");

                        continue;
                    }

                    yield return new SubscriptionState()
                    {
                        Query = query,
                        ChangeVectorForNextBatchStartingPoint = changeVectorForNextBatchStartingPoint,
                        SubscriptionName = subscriptionName,
                        SubscriptionId = subscriptionId,
                        MentorNode = mentorNode,
                        NodeTag = nodeTag,
                        LastBatchAckTime = lastBatchAckTime,
                        LastClientConnectionTime = lastClientConnectionTime,
                        Disabled = disabled
                    };
                }
            }
        }

        private unsafe void SetBuffer(UnmanagedJsonParser parser, LazyStringValue value)
        {
            parser.SetBuffer(value.Buffer, value.Size);
        }

        private IEnumerable<(string key, long index, BlittableJsonReaderObject value)> InternalGetCompareExchangeValues(INewCompareExchangeActions actions)
        {
            var state = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(_context, state, "Import/CompareExchange"))
            using (var builder = new BlittableJsonDocumentBuilder(actions.GetContextForNewCompareExchangeValue(), 
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "Import/CompareExchange", parser, state))
            {
                foreach (var reader in ReadArray())
                {
                    using (reader)
                    {
                        if (reader.TryGet("Key", out string key) == false ||
                            reader.TryGet("Value", out LazyStringValue value) == false)
                        {
                            _result.CompareExchange.ErroredCount++;
                            _result.AddWarning("Could not read compare exchange entry.");

                            continue;
                        }

                        using (value)
                        {
                            builder.ReadNestedObject();
                            SetBuffer(parser, value);
                            parser.Read();
                            builder.Read();
                            builder.FinalizeDocument();
                            yield return (key, 0, builder.CreateReader());

                            builder.Renew("import/cmpxchg", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                }
            }
        }

        private IEnumerable<string> InternalGetCompareExchangeTombstones()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    if (reader.TryGet("Key", out string key) == false)
                    {
                        _result.CompareExchange.ErroredCount++;
                        _result.AddWarning("Could not read compare exchange tombstone.");

                        continue;
                    }

                    yield return key;
                }
            }
        }

        private IEnumerable<CounterGroupDetail> InternalGetCounterValues(ICounterActions actions)
        {
            foreach (var reader in ReadArray(actions))
            {
                if (reader.TryGet(nameof(CounterItem.DocId), out LazyStringValue docId) == false ||
                    reader.TryGet(nameof(CounterItem.Batch.Values), out BlittableJsonReaderObject values) == false ||
                    reader.TryGet(nameof(CounterItem.ChangeVector), out LazyStringValue cv) == false)
                {
                    _result.Counters.ErroredCount++;
                    _result.AddWarning("Could not read counter entry.");

                    continue;
                }

                values = ConvertToBlob(values, actions);

                actions.RegisterForDisposal(reader);

                yield return new CounterGroupDetail
                {
                    DocumentId = docId,
                    ChangeVector = cv,
                    Values = values
                };
            }
        }

        private unsafe BlittableJsonReaderObject ConvertToBlob(BlittableJsonReaderObject values, ICounterActions actions)
        {
            var scopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

            try
            {
                var context = actions.GetContextForNewDocument();
                Debug.Assert(context == values._context);
                values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counterValues);

                counterValues.Modifications = new DynamicJsonValue(counterValues);
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < counterValues.Count; i++)
                {
                    counterValues.GetPropertyByIndex(i, ref prop);

                    if (prop.Value is LazyStringValue)
                        continue; //deleted counter

                    var arr = (BlittableJsonReaderArray)prop.Value;
                    var sizeToAllocate = CountersStorage.SizeOfCounterValues * arr.Length / 2;

                    scopes.Add(context.Allocator.Allocate(sizeToAllocate, out var newVal));

                    for (int j = 0; j < arr.Length; j += 2)
                    {
                        var newEntry = (CountersStorage.CounterValues*)newVal.Ptr + j / 2;
                        newEntry->Value = (long)arr[j];
                        newEntry->Etag = (long)arr[j + 1];
                    }

                    counterValues.Modifications[prop.Name] = new BlittableJsonReaderObject.RawBlob
                    {
                        Ptr = newVal.Ptr,
                        Length = newVal.Length
                    };
                }

                return context.ReadObject(values, null);
            }
            finally
            {
                foreach (var scope in scopes)
                {
                    scope.Dispose();
                }
            }
        }

        public long SkipType(DatabaseItemType type, Action<long> onSkipped, CancellationToken token)
        {
            switch (type)
            {
                case DatabaseItemType.None:
                    return 0;
                case DatabaseItemType.Documents:
                case DatabaseItemType.RevisionDocuments:
                case DatabaseItemType.Tombstones:
                case DatabaseItemType.Conflicts:
                case DatabaseItemType.Indexes:
                case DatabaseItemType.Identities:
                case DatabaseItemType.CompareExchange:
                case DatabaseItemType.Subscriptions:
                case DatabaseItemType.CompareExchangeTombstones:
                case DatabaseItemType.LegacyDocumentDeletions:
                case DatabaseItemType.LegacyAttachmentDeletions:
#pragma warning disable 618
                case DatabaseItemType.Counters:
#pragma warning restore 618
                case DatabaseItemType.CounterGroups:
                    return SkipArray(onSkipped, null, token);
                case DatabaseItemType.DatabaseRecord:
                    return SkipObject(onSkipped);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadDocuments(actions);
        }

        public IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadDocuments(actions);
        }

        public IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions)
        {
            return ReadLegacyAttachments(actions);
        }

        public IEnumerable<string> GetLegacyAttachmentDeletions()
        {
            foreach (var id in ReadLegacyDeletions())
                yield return GetLegacyAttachmentId(id);
        }

        public IEnumerable<string> GetLegacyDocumentDeletions()
        {
            return ReadLegacyDeletions();
        }

        public IEnumerable<Tombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadTombstones(actions);
        }

        public IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadConflicts(actions);
        }

        public IEnumerable<IndexDefinitionAndType> GetIndexes()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    IndexType type;
                    object indexDefinition;

                    try
                    {
                        indexDefinition = IndexProcessor.ReadIndexDefinition(reader, _buildVersionType, out type);
                    }
                    catch (Exception e)
                    {
                        _result.Indexes.ErroredCount++;
                        _result.AddWarning($"Could not read index definition. Message: {e.Message}");

                        continue;
                    }

                    yield return new IndexDefinitionAndType
                    {
                        Type = type,
                        IndexDefinition = indexDefinition
                    };
                }
            }
        }

        public IEnumerable<(string Prefix, long Value, long Index)> GetIdentities()
        {
            return InternalGetIdentities();
        }

        private IEnumerable<(string Prefix, long Value, long Index)> InternalGetIdentities()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    if (reader.TryGet("Key", out string identityKey) == false ||
                        reader.TryGet("Value", out string identityValueString) == false ||
                        long.TryParse(identityValueString, out long identityValue) == false)
                    {
                        _result.Identities.ErroredCount++;
                        _result.AddWarning("Could not read identity.");

                        continue;
                    }

                    yield return (identityKey, identityValue, 0);
                }
            }
        }

        private unsafe string ReadType()
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of object when reading type", _peepingTomStream, _parser);

            if (_state.CurrentTokenType == JsonParserToken.EndObject)
                return null;

            if (_state.CurrentTokenType != JsonParserToken.String)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected property type to be string, but was " + _state.CurrentTokenType, _peepingTomStream, _parser);

            return _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize).ToString();
        }

        private void ReadObject(BlittableJsonDocumentBuilder builder)
        {
            UnmanagedJsonParserHelper.ReadObject(builder, _peepingTomStream, _parser, _buffer);

            _totalObjectsRead.Add(builder.SizeInBytes, SizeUnit.Bytes);
        }

        private void ReadObject(Action<BlittableJsonReaderObject> readAction)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start object, got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            using (var builder = CreateBuilder(_context))
            {
                _context.CachedProperties.NewDocument();
                ReadObject(builder);

                using (var reader = builder.CreateReader())
                {
                    readAction(reader);
                }
            }
        }

        private long ReadBuildVersion()
        {
            var type = ReadType();
            if (type == null)
                return 0;

            if (type.Equals("BuildVersion", StringComparison.OrdinalIgnoreCase) == false)
            {
                _currentType = GetType(type);
                return 0;
            }

            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json.", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.Integer)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected integer BuildVersion, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            return _state.Long;
        }

        private long SkipArray(Action<long> onSkipped, Action<BlittableJsonReaderObject> additionalSkip, CancellationToken token)
        {
            var count = 0L;
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    token.ThrowIfCancellationRequested();
                    additionalSkip?.Invoke(reader);

                    count++; //skipping
                    onSkipped?.Invoke(count);
                }
            }

            return count;
        }

        private void MaySkipBlob(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(Constants.Documents.Blob.Size, out int size))
                Skip(size);
        }

        private void SkipAttachmentStream(BlittableJsonReaderObject data)
        {
            if (data.TryGet(nameof(AttachmentName.Hash), out LazyStringValue _) == false ||
                data.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                data.TryGet(nameof(DocumentItem.AttachmentStream.Tag), out LazyStringValue _) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            Skip(size);
        }

        private void Skip(long size)
        {
            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(32 * 1024, size);
                var read = _parser.Skip(sizeToRead);
                if (read.Done == false)
                {
                    var read2 = _peepingTomStream.Read(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }
                size -= read.BytesRead;
            }
        }

        private long SkipObject(Action<long> onSkipped = null)
        {
            var count = 1;
            ReadObject(reader => { });
            onSkipped?.Invoke(count);
            return count;
        }

        private IEnumerable<BlittableJsonReaderObject> ReadArray(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var builder = CreateBuilder(context);

            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading array", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (context != oldContext)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context);
                        }
                    }

                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    context.CachedProperties.NewDocument();

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet(DocumentItem.ExportDocumentType.Key, out string type) &&
                        type == DocumentItem.ExportDocumentType.Attachment)
                    {
                        // skip document attachments, documents with attachments are handled separately
                        SkipAttachmentStream(data);
                        continue;
                    }

                    yield return data;
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        private IEnumerable<string> ReadLegacyDeletions()
        {
            foreach (var item in ReadArray())
            {
                if (item.TryGet("Key", out string key) == false)
                    continue;

                yield return key;
            }
        }

        private IEnumerable<DocumentItem> ReadLegacyAttachments(INewDocumentActions actions)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var builder = CreateBuilder(context);
            var modifier = new BlittableMetadataModifier(context);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading legacy attachments", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (oldContext != context)
                        {
                            builder.Dispose();
                            modifier.Dispose();
                            modifier = new BlittableMetadataModifier(context);
                            builder = CreateBuilder(context, modifier);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    _context.CachedProperties.NewDocument();

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var attachment = new DocumentItem.AttachmentStream
                    {
                        Stream = actions.GetTempStream()
                    };

                    var attachmentInfo = ProcessLegacyAttachment(context, data, ref attachment);
                    if (ShouldSkip(attachmentInfo))
                        continue;

                    var dummyDoc = new DocumentItem
                    {
                        Document = new Document
                        {
                            Data = WriteDummyDocumentForAttachment(context, attachmentInfo),
                            Id = attachmentInfo.Id,
                            ChangeVector = string.Empty,
                            Flags = DocumentFlags.HasAttachments,
                            NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler,
                            LastModified = _database.Time.GetUtcNow(),
                        },
                        Attachments = new List<DocumentItem.AttachmentStream>
                        {
                            attachment
                        }
                    };

                    yield return dummyDoc;
                }
            }
            finally
            {
                builder.Dispose();
                modifier.Dispose();
            }
        }

        private static bool ShouldSkip(LegacyAttachmentDetails attachmentInfo)
        {
            if (attachmentInfo.Metadata.TryGet("Raven-Delete-Marker", out bool deleted) && deleted)
                return true;

            return attachmentInfo.Key.EndsWith(".deleting") || attachmentInfo.Key.EndsWith(".downloading");
        }

        public static BlittableJsonReaderObject WriteDummyDocumentForAttachment(DocumentsOperationContext context, LegacyAttachmentDetails details)
        {
            var attachment = new DynamicJsonValue
            {
                ["Name"] = details.Key,
                ["Hash"] = details.Hash,
                ["ContentType"] = string.Empty,
                ["Size"] = details.Size,
            };
            var attachments = new DynamicJsonArray();
            attachments.Add(attachment);
            var metadata = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = "@files",
                [Constants.Documents.Metadata.Attachments] = attachments,
                [Constants.Documents.Metadata.LegacyAttachmentsMetadata] = details.Metadata
            };
            var djv = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = metadata,
            };

            return context.ReadObject(djv, details.Id);
        }

        private IEnumerable<DocumentItem> ReadDocuments(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var legacyImport = _buildVersionType == BuildVersionType.V3;
            var modifier = new BlittableMetadataModifier(context, legacyImport, _readLegacyEtag, _operateOnTypes);
            var builder = CreateBuilder(context, modifier);
            try
            {
                List<DocumentItem.AttachmentStream> attachments = null;
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (oldContext != context)
                        {
                            builder.Dispose();
                            modifier.Dispose();
                            modifier = new BlittableMetadataModifier(context, legacyImport, _readLegacyEtag, _operateOnTypes)
                            {
                                FirstEtagOfLegacyRevision = modifier.FirstEtagOfLegacyRevision,
                                LegacyRevisionsCount = modifier.LegacyRevisionsCount
                            };
                            builder = CreateBuilder(context, modifier);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    context.CachedProperties.NewDocument();

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet(DocumentItem.ExportDocumentType.Key, out string type))
                    {
                        if (type != DocumentItem.ExportDocumentType.Attachment)
                        {
                            var msg = $"Ignoring an item of type `{type}`. " + data;
                            if (_log.IsOperationsEnabled)
                                _log.Operations(msg);
                            _result.AddWarning(msg);
                            continue;
                        }

                        if (attachments == null)
                            attachments = new List<DocumentItem.AttachmentStream>();

                        var attachment = new DocumentItem.AttachmentStream
                        {
                            Stream = actions.GetTempStream()
                        };
                        ProcessAttachmentStream(context, data, ref attachment);
                        attachments.Add(attachment);
                        continue;
                    }

                    if (legacyImport)
                    {
                        if (modifier.Id.Contains(HiLoHandler.RavenHiloIdPrefix))
                        {
                            data.Modifications = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = CollectionName.HiLoCollection
                                }
                            };
                        }
                    }

                    if (data.Modifications != null)
                    {
                        data = context.ReadObject(data, modifier.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }

                    _result.LegacyLastDocumentEtag = modifier.LegacyEtag;

                    yield return new DocumentItem
                    {
                        Document = new Document
                        {
                            Data = data,
                            Id = context.GetLazyString(modifier.Id),
                            ChangeVector = modifier.ChangeVector,
                            Flags = modifier.Flags,
                            NonPersistentFlags = modifier.NonPersistentFlags,
                            LastModified = modifier.LastModified ?? _database.Time.GetUtcNow(),
                        },
                        Attachments = attachments
                    };
                    attachments = null;
                }
            }
            finally
            {
                builder.Dispose();
                modifier.Dispose();
            }
        }

        private IEnumerable<Tombstone> ReadTombstones(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var builder = CreateBuilder(context);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (oldContext != context)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    _context.CachedProperties.NewDocument();

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var tombstone = new Tombstone();
                    if (data.TryGet("Key", out tombstone.LowerId) &&
                        data.TryGet(nameof(Tombstone.Type), out string type) &&
                        data.TryGet(nameof(Tombstone.Collection), out tombstone.Collection) &&
                        data.TryGet(nameof(Tombstone.LastModified), out tombstone.LastModified))
                    {
                        if (Enum.TryParse<Tombstone.TombstoneType>(type, out var tombstoneType) == false)
                        {
                            var msg = $"Ignoring a tombstone of type `{type}` which is not supported in 4.0. ";
                            if (_log.IsOperationsEnabled)
                                _log.Operations(msg);

                            _result.Tombstones.ErroredCount++;
                            _result.AddWarning(msg);
                            continue;
                        }

                        tombstone.Type = tombstoneType;

                        if (data.TryGet(nameof(Tombstone.Flags), out string flags))
                        {
                            if (Enum.TryParse<DocumentFlags>(flags, out var tombstoneFlags) == false)
                            {
                                var msg = $"Ignoring a tombstone because it couldn't parse its flags: {flags}";
                                if (_log.IsOperationsEnabled)
                                    _log.Operations(msg);

                                _result.Tombstones.ErroredCount++;
                                _result.AddWarning(msg);
                                continue;
                            }

                            tombstone.Flags = tombstoneFlags;
                        }

                        yield return tombstone;
                    }
                    else
                    {
                        var msg = "Ignoring an invalid tombstone which you try to import. " + data;
                        if (_log.IsOperationsEnabled)
                            _log.Operations(msg);

                        _result.Tombstones.ErroredCount++;
                        _result.AddWarning(msg);
                    }
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        private IEnumerable<DocumentConflict> ReadConflicts(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var builder = CreateBuilder(context);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (oldContext != context)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    _context.CachedProperties.NewDocument();

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var conflict = new DocumentConflict();
                    if (data.TryGet(nameof(DocumentConflict.Id), out conflict.Id) &&
                        data.TryGet(nameof(DocumentConflict.Collection), out conflict.Collection) &&
                        data.TryGet(nameof(DocumentConflict.Flags), out string flags) &&
                        data.TryGet(nameof(DocumentConflict.ChangeVector), out conflict.ChangeVector) &&
                        data.TryGet(nameof(DocumentConflict.Etag), out conflict.Etag) &&
                        data.TryGet(nameof(DocumentConflict.LastModified), out conflict.LastModified) &&
                        data.TryGet(nameof(DocumentConflict.Doc), out conflict.Doc))
                    {
                        conflict.Flags = Enum.Parse<DocumentFlags>(flags);
                        if (conflict.Doc != null) // This is null for conflict that was generated from tombstone
                            conflict.Doc = context.ReadObject(conflict.Doc, conflict.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        yield return conflict;
                    }
                    else
                    {
                        var msg = "Ignoring an invalid conflict which you try to import. " + data;
                        if (_log.IsOperationsEnabled)
                            _log.Operations(msg);

                        _result.Conflicts.ErroredCount++;
                        _result.AddWarning(msg);
                    }
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        internal unsafe LegacyAttachmentDetails ProcessLegacyAttachment(
            DocumentsOperationContext context,
            BlittableJsonReaderObject data,
            ref DocumentItem.AttachmentStream attachment)
        {
            if (data.TryGet("Key", out string key) == false)
            {
                throw new ArgumentException("The key of legacy attachment is missing its key property.");
            }

            if (data.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
            {
                throw new ArgumentException($"Metadata of legacy attachment with key={key} is missing");
            }

            if (data.TryGet("Data", out string base64data) == false)
            {
                throw new ArgumentException($"Data of legacy attachment with key={key} is missing");
            }

            if (_readLegacyEtag && data.TryGet("Etag", out string etag))
            {
                _result.LegacyLastAttachmentEtag = etag;
            }

            var memoryStream = new MemoryStream();

            fixed (char* pdata = base64data)
            {
                memoryStream.SetLength(Base64.FromBase64_ComputeResultLength(pdata, base64data.Length));
                fixed (byte* buffer = memoryStream.GetBuffer())
                    Base64.FromBase64_Decode(pdata, base64data.Length, buffer, (int)memoryStream.Length);
            }

            memoryStream.Position = 0;

            return GenerateLegacyAttachmentDetails(context, memoryStream, key, metadata, ref attachment);
        }

        public static string GetLegacyAttachmentId(string key)
        {
            return $"{DummyDocumentPrefix}{key}";
        }

        public static LegacyAttachmentDetails GenerateLegacyAttachmentDetails(
            DocumentsOperationContext context,
            Stream decodedStream,
            string key,
            BlittableJsonReaderObject metadata,
            ref DocumentItem.AttachmentStream attachment)
        {
            var stream = attachment.Stream;
            var hash = AsyncHelpers.RunSync(() => AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, decodedStream, stream, CancellationToken.None));
            attachment.Stream.Flush();
            var lazyHash = context.GetLazyString(hash);
            attachment.Base64HashDispose = Slice.External(context.Allocator, lazyHash, out attachment.Base64Hash);
            var tag = $"{DummyDocumentPrefix}{key}{RecordSeparator}d{RecordSeparator}{key}{RecordSeparator}{hash}{RecordSeparator}";
            var lazyTag = context.GetLazyString(tag);
            attachment.TagDispose = Slice.External(context.Allocator, lazyTag, out attachment.Tag);
            var id = GetLegacyAttachmentId(key);
            var lazyId = context.GetLazyString(id);

            attachment.Data = context.ReadObject(metadata, id);
            return new LegacyAttachmentDetails
            {
                Id = lazyId,
                Hash = hash,
                Key = key,
                Size = attachment.Stream.Length,
                Tag = tag,
                Metadata = attachment.Data
            };
        }

        public struct LegacyAttachmentDetails
        {
            public LazyStringValue Id;
            public string Hash;
            public string Key;
            public long Size;
            public string Tag;
            public BlittableJsonReaderObject Metadata;
        }

        private const char RecordSeparator = (char)SpecialChars.RecordSeparator;
        private const string DummyDocumentPrefix = "files/";

        public unsafe void ProcessAttachmentStream(DocumentsOperationContext context, BlittableJsonReaderObject data, ref DocumentItem.AttachmentStream attachment)
        {
            if (data.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                data.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                data.TryGet(nameof(DocumentItem.AttachmentStream.Tag), out LazyStringValue tag) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            if (_writeBuffer == null)
                _returnWriteBuffer = _context.GetManagedBuffer(out _writeBuffer);

            attachment.Data = data;
            attachment.Base64HashDispose = Slice.External(context.Allocator, hash, out attachment.Base64Hash);
            attachment.TagDispose = Slice.External(context.Allocator, tag, out attachment.Tag);

            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(_writeBuffer.Length, size);
                var read = _parser.Copy(_writeBuffer.Pointer, sizeToRead);
                attachment.Stream.Write(_writeBuffer.Buffer.Array, _writeBuffer.Buffer.Offset, read.BytesRead);
                if (read.Done == false)
                {
                    var read2 = _peepingTomStream.Read(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }
                size -= read.BytesRead;
            }
            attachment.Stream.Flush();
        }

        private BlittableJsonDocumentBuilder CreateBuilder(JsonOperationContext context, BlittableMetadataModifier modifier)
        {
            return new BlittableJsonDocumentBuilder(context,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "import/object", _parser, _state,
                modifier: modifier);
        }

        private BlittableJsonDocumentBuilder CreateBuilder(JsonOperationContext context)
        {
            return new BlittableJsonDocumentBuilder(context,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "import/object", _parser, _state);
        }

        private DatabaseItemType GetType(string type)
        {
            if (type == null)
                return DatabaseItemType.None;

            if (type.Equals(nameof(DatabaseItemType.DatabaseRecord), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.DatabaseRecord;

            if (type.Equals("Docs", StringComparison.OrdinalIgnoreCase) ||
                type.Equals("Results", StringComparison.OrdinalIgnoreCase)) // reading from stream/docs endpoint
                return DatabaseItemType.Documents;

            if (type.Equals(nameof(DatabaseItemType.RevisionDocuments), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.RevisionDocuments;

            if (type.Equals(nameof(DatabaseItemType.Tombstones), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Tombstones;

            if (type.Equals(nameof(DatabaseItemType.Conflicts), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Conflicts;

            if (type.Equals(nameof(DatabaseItemType.Indexes), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Indexes;

            if (type.Equals(nameof(DatabaseItemType.Identities), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Identities;

            if (type.Equals(nameof(DatabaseItemType.Subscriptions), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Subscriptions;

            if (type.Equals(nameof(DatabaseItemType.CompareExchange), StringComparison.OrdinalIgnoreCase) ||
                type.Equals("CmpXchg", StringComparison.OrdinalIgnoreCase)) //support the old name
                return DatabaseItemType.CompareExchange;

            if (type.Equals(nameof(DatabaseItemType.CounterGroups), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.CounterGroups;

#pragma warning disable 618
            if (type.Equals(nameof(DatabaseItemType.Counters), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Counters;
#pragma warning restore 618

            if (type.Equals(nameof(DatabaseItemType.CompareExchangeTombstones), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.CompareExchangeTombstones;

            if (type.Equals("Attachments", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyAttachments;

            if (type.Equals("DocsDeletions", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyDocumentDeletions;

            if (type.Equals("AttachmentsDeletions", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyAttachmentDeletions;

            return DatabaseItemType.Unknown;
        }

        public void Dispose()
        {
            _peepingTomStream.Dispose();
        }
    }
}

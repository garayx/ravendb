﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Queries;

public abstract class AbstractShardedQueryProcessor<TCommand, TResult, TCombinedResult> where TCommand : RavenCommand<TResult>
{
    const string LimitToken = "__raven_limit";

    private Dictionary<int, BlittableJsonReaderObject> _queryTemplates;
    private Dictionary<int, TCommand> _commands;
    private readonly bool _metadataOnly;
    private readonly bool _indexEntriesOnly;
    private readonly string _raftUniqueRequestId;
    private readonly HashSet<int> _filteredShardIndexes;

    protected readonly TransactionOperationContext Context;
    protected readonly ShardedDatabaseRequestHandler RequestHandler;
    protected readonly IndexQueryServerSide Query;
    protected readonly CancellationToken Token;
    protected readonly bool IsMapReduceIndex;
    protected readonly bool IsAutoMapReduceQuery;
    protected readonly long? ExistingResultEtag;

    protected AbstractShardedQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, bool metadataOnly, bool indexEntriesOnly,
        long? existingResultEtag, CancellationToken token)
    {
        RequestHandler = requestHandler;
        Query = query;
        _metadataOnly = metadataOnly;
        _indexEntriesOnly = indexEntriesOnly;
        Token = token;
        Context = context;
        ExistingResultEtag = existingResultEtag;

        IsMapReduceIndex = Query.Metadata.IndexName != null && (RequestHandler.DatabaseContext.Indexes.GetIndex(Query.Metadata.IndexName)?.Type.IsMapReduce() ?? false);
        IsAutoMapReduceQuery = Query.Metadata.IsDynamic && Query.Metadata.IsGroupBy;

        _raftUniqueRequestId = RequestHandler.GetRaftRequestIdFromQuery() ?? RaftIdGenerator.NewId();

        if (Query.QueryParameters != null && Query.QueryParameters.TryGetMember(Constants.Documents.Querying.Sharding.ShardContextParameterName, out object filter))
        {
            // User can define a query parameter ("__shardContext") which is an id or an array
            // that contains the ids whose shards the query should be limited to.
            // Advanced: Optimization if user wants to run a query and knows what shards it is on. Such as:
            // from Orders where State = $state and User = $user where all the orders are on the same share as the user

            _filteredShardIndexes = new HashSet<int>();
            switch (filter)
            {
                case LazyStringValue id:
                    _filteredShardIndexes.Add(RequestHandler.DatabaseContext.GetShardNumberFor(context, id));
                    break;
                case BlittableJsonReaderArray arr:
                {
                    for (int i = 0; i < arr.Length; i++)
                    {
                        var it = arr.GetStringByIndex(i);
                        _filteredShardIndexes.Add(RequestHandler.DatabaseContext.GetShardNumberFor(context, it));
                    }
                    break;
                }
                default:
                    throw new NotSupportedException($"Unknown type of a shard context query parameter: {filter.GetType().Name}");
            }
        }
        else
        {
            _filteredShardIndexes = null;
        }
    }

    public abstract Task<TCombinedResult> ExecuteShardedOperations();

    protected int[] GetShardNumbers(Dictionary<int, TCommand> commands)
    {
        return _filteredShardIndexes == null ? commands.Keys.ToArray() : commands.Keys.Intersect(_filteredShardIndexes).ToArray();
    }

    public virtual ValueTask InitializeAsync()
    {
        AssertQueryExecution();

        // now we have the index query, we need to process that and decide how to handle this.
        // There are a few different modes to handle:
        // - For collection queries that specify startsWith by id(), we need to send to all shards
        // - For collection queries without any where clause, we need to send to all shards
        // - For indexes, we sent to all shards
        
        var queryTemplate = Query.ToJson(Context);

        if (Query.Metadata.IsCollectionQuery && Query.Metadata.DeclaredFunctions is null or { Count: 0})
        {
            // * For collection queries that specify ids, we can turn that into a set of loads that 
            //   will hit the known servers

            (List<Slice> ids, string _) = Query.ExtractIdsFromQuery(RequestHandler.ServerStore, Context.Allocator, RequestHandler.DatabaseContext.DatabaseName);

            if (ids != null)
            {
                _queryTemplates = GenerateLoadByIdQueries(ids);
            }
        }

        if (_queryTemplates == null)
        {
            RewriteQueryIfNeeded(ref queryTemplate);

            _queryTemplates = new(RequestHandler.DatabaseContext.ShardCount);

            foreach (var shardNumber in RequestHandler.DatabaseContext.ShardsTopology.Keys)
            {
                _queryTemplates.Add(shardNumber, queryTemplate);
            }
        }

        return ValueTask.CompletedTask;
    }

    private Dictionary<int, TCommand> CreateQueryCommands(Dictionary<int, BlittableJsonReaderObject> preProcessedQueries)
    {
        var commands = new Dictionary<int, TCommand>(preProcessedQueries.Count);

        foreach (var (shard, query) in preProcessedQueries)
        {
            commands.Add(shard, CreateCommand(query));
        }

        return commands;
    }

    public Dictionary<int, TCommand> GetOperationCommands()
    {
        return _commands ??= CreateQueryCommands(_queryTemplates);
    }

    protected abstract TCommand CreateCommand(BlittableJsonReaderObject query);

    protected ShardedQueryCommand CreateShardedQueryCommand(BlittableJsonReaderObject query)
    {
        return new ShardedQueryCommand(Context.ReadObject(query, "query"), Query, _metadataOnly, _indexEntriesOnly, Query.Metadata.IndexName,
            canReadFromCache: ExistingResultEtag != null, _raftUniqueRequestId);
    }

    protected virtual void AssertQueryExecution()
    {
        AssertUsingCustomSorters();
    }

    private void AssertUsingCustomSorters()
    {
        if (Query.Metadata.OrderBy == null)
            return;

        foreach (var field in Query.Metadata.OrderBy)
        {
            if (field.OrderingType == OrderByFieldType.Custom)
                throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
        }
    }

    private void RewriteQueryIfNeeded(ref BlittableJsonReaderObject queryTemplate)
    {
        var rewriteForPaging = Query.Offset is > 0;
        var rewriteForProjection = true;

        var query = Query.Metadata.Query;
        if (query.Select?.Count > 0 == false &&
            query.SelectFunctionBody.FunctionText == null)
        {
            rewriteForProjection = false;
        }

        if (Query.Metadata.IndexName == null || IsMapReduceIndex == false)
        {
            rewriteForProjection = false;
        }

        if (rewriteForPaging == false && rewriteForProjection == false)
            return;

        var clone = Query.Metadata.Query.ShallowCopy();

        DynamicJsonValue modifications = new(queryTemplate);

        if (rewriteForPaging)
        {
            // For paging queries, we modify the limits on the query to include all the results from all
            // shards if there is an offset. But if there isn't an offset, we can just get the limit from
            // each node and then merge them

            clone.Offset = null; // sharded queries has to start from 0 on all nodes
            clone.Limit = new ValueExpression(LimitToken, ValueTokenType.Parameter);

            DynamicJsonValue modifiedArgs;
            if (queryTemplate.TryGet(nameof(IndexQuery.QueryParameters), out BlittableJsonReaderObject args))
            {
                modifiedArgs = new DynamicJsonValue(args);
                args.Modifications = modifiedArgs;
            }
            else
            {
                modifications[nameof(IndexQuery.QueryParameters)] = modifiedArgs = new DynamicJsonValue();
            }

            var limit = ((Query.Limit ?? 0) + (Query.Offset ?? 0)) * (long)RequestHandler.DatabaseContext.ShardCount;

            if (limit > int.MaxValue) // overflow
                limit = int.MaxValue;

            modifiedArgs[LimitToken] = limit;

            modifications.Remove(nameof(IndexQueryServerSide.Start));
            modifications.Remove(nameof(IndexQueryServerSide.PageSize));
        }

        if (rewriteForProjection)
        {
            // If we have a projection in a map-reduce index,
            // the shards will send the query result and the orchestrator will re-reduce and apply the projection
            // in that case we must send the query without the projection

            if (query.Load is { Count: > 0 })
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "https://issues.hibernatingrhinos.com/issue/RavenDB-17887");
                throw new NotSupportedInShardingException("Loading a document inside a projection from a map-reduce index isn't supported");
            }

            clone.Select = null;
            clone.SelectFunctionBody = default;
            clone.DeclaredFunctions = null;
        }

        modifications[nameof(IndexQuery.Query)] = clone.ToString();

        queryTemplate.Modifications = modifications;

        queryTemplate = Context.ReadObject(queryTemplate, "modified-query");
    }

    private Dictionary<int, BlittableJsonReaderObject> GenerateLoadByIdQueries(IEnumerable<Slice> ids)
    {
        const string listParameterName = "p0";

        var documentQuery = new DocumentQuery<dynamic>(null, null, Query.Metadata.CollectionName, isGroupBy: false, fromAlias: Query.Metadata.Query.From.Alias?.ToString());
        documentQuery.WhereIn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, Enumerable.Empty<object>());
        
        IncludeBuilder includeBuilder = null;

        if (Query.Metadata.Includes is { Length: > 0 })
        {
            includeBuilder = new IncludeBuilder
            {
                DocumentsToInclude = new HashSet<string>(Query.Metadata.Includes)
            };
        }

        if (Query.Metadata.RevisionIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.RevisionsToIncludeByChangeVector = Query.Metadata.RevisionIncludes.RevisionsChangeVectorsPaths;
            includeBuilder.RevisionsToIncludeByDateTime = Query.Metadata.RevisionIncludes.RevisionsBeforeDateTime;
        }

        if (Query.Metadata.TimeSeriesIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.TimeSeriesToIncludeBySourceAlias = Query.Metadata.TimeSeriesIncludes.TimeSeries;
        }

        if (Query.Metadata.CounterIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.CountersToIncludeBySourcePath = new Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)>(StringComparer.OrdinalIgnoreCase);

            foreach (var counterIncludes in Query.Metadata.CounterIncludes.Counters)
            {
                var name = counterIncludes.Key;

                if (includeBuilder.CountersToIncludeBySourcePath.TryGetValue(name, out var counters) == false)
                    includeBuilder.CountersToIncludeBySourcePath[name] = counters = (false, new HashSet<string>(StringComparer.OrdinalIgnoreCase));

                foreach (string counterName in counterIncludes.Value)
                {
                    counters.CountersToInclude.Add(counterName);
                }
            }
        }

        if (includeBuilder != null)
            documentQuery.Include(includeBuilder);

        var queryText = documentQuery.ToString();

        if (Query.Metadata.Query.Select is { Count: > 0 })
        {
            var selectStartPosition = Query.Metadata.QueryText.IndexOf("select", StringComparison.OrdinalIgnoreCase);

            var selectClause = Query.Metadata.QueryText.Substring(selectStartPosition);

            queryText += $" {selectClause}";
        }
        
        Dictionary<int, BlittableJsonReaderObject> queryTemplates = new();

        var shards = ShardLocator.GetDocumentIdsByShards(Context, RequestHandler.DatabaseContext, ids);

        foreach ((int shardId, ShardLocator.IdsByShard<Slice> documentIds) in shards)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 have a way to turn the _query into a json file and then we'll modify that, instead of building it manually");

            var q = new DynamicJsonValue
            {
                [nameof(IndexQuery.QueryParameters)] = new DynamicJsonValue
                {
                    [listParameterName] = GetIds()
                },
                [nameof(IndexQuery.Query)] = queryText
            };

            queryTemplates[shardId] = Context.ReadObject(q, "query");

            IEnumerable<string> GetIds()
            {
                foreach (var idAsAlice in documentIds.Ids)
                {
                    yield return idAsAlice.ToString();
                }
            }
        }

        return queryTemplates;
    }

    protected async Task HandleMissingDocumentIncludes<T, TIncludes>(HashSet<string> missingIncludes, QueryResult<List<T>, List<TIncludes>> result)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(Context, RequestHandler.DatabaseContext, missingIncludes);
        var missingIncludesOp = new FetchDocumentsFromShardsOperation(Context, RequestHandler, missingIncludeIdsByShard, null, null, counterIncludes: default, null, null, null, _metadataOnly);
        var missingResult = await RequestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, Token);

        var blittableIncludes = result.Includes as List<BlittableJsonReaderObject>;
        var documentIncludes = result.Includes as List<Document>;

        foreach (var (_, missing) in missingResult.Result.Documents)
        {
            if (missing == null)
                continue;

            if (blittableIncludes != null)
                blittableIncludes.Add(missing);
            else if (documentIncludes != null)
            {
                if (missing.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                {
                    if (metadata.TryGet(Constants.Documents.Metadata.Id, out LazyStringValue id))
                    {
                        documentIncludes.Add(new Document
                        {
                            Id = id,
                            Data = missing
                        });
                    }
                }
            }
            else
                throw new NotSupportedException($"Unknown includes type: {result.Includes.GetType().FullName}");
        }
    }

    protected async Task WaitForRaftIndexIfNeededAsync(long? raftCommandIndex)
    {
        if (IsAutoMapReduceQuery && raftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(raftCommandIndex.Value, Token);
        }
    }
}

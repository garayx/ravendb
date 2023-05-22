using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Xunit;
using static Raven.Server.Utils.MetricCacher.Keys;
// ReSharper disable InconsistentNaming

namespace FastTests;

public partial class RavenTestBase
{
    public class ReshardingTestBase
    {
        private readonly RavenTestBase _parent;

        public ReshardingTestBase(RavenTestBase parent)
        {
            _parent = parent;
        }

        public async Task<int> StartMovingShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = _parent.Sharding.GetBucket(record.Sharding, id);
            PrefixedShardingSetting prefixed = null;
            foreach (var setting in record.Sharding.Prefixed)
            {
                if (id.StartsWith(setting.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    prefixed = setting;
                    break;
                }
            }

            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var moveToShard = toShard ?? (prefixed != null 
                ? ShardingTestBase.GetNextSortedShardNumber(prefixed, shardNumber)
                : ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber));
            //Console.WriteLine($"___ START reshard from '{shardNumber}' to '{moveToShard}' ___");
            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
            {
                Assert.True(await session.Advanced.ExistsAsync(id));
            }

            foreach (var server in servers)
            {
                try
                {
                    await server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, moveToShard);
                    break;
                }
                catch
                {
                    //
                }
            }
                
            var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, moveToShard), timeout: 30_000);
            Assert.True(exists, $"{id} wasn't found at shard {moveToShard}");
            //   Console.WriteLine($"end moving from '{shardNumber}' to '{moveToShard}'");
            return bucket;
        }

        public async Task WaitForMigrationComplete(IDocumentStore store, int bucket)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60 * 15)))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                while (record.Sharding.BucketMigrations.ContainsKey(bucket))
                {
                    await Task.Delay(250, cts.Token);
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                }
            }
        }

        public async Task MoveShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null)
        {
            try
            {
                servers ??= _parent.GetServers();
                var bucket = await StartMovingShardForId(store, id, toShard, servers);
                await WaitForMigrationComplete(store, bucket);
            }
            catch (Exception e)
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var sharding = store.Conventions.Serialization.DefaultConverter.ToBlittable(record.Sharding, ctx).ToString();
                    throw new InvalidOperationException(
                        $"Failed to completed the migration for {id}{Environment.NewLine}{sharding}{Environment.NewLine}{_parent.Cluster.CollectLogsFromNodes(servers ?? new List<RavenServer> { _parent.Server })}",
                        e);
                }
            }
            finally
            {
              //  Console.WriteLine("___ END reshard ___");

            }
        }

        public async Task StartManualShardMigrationForId(IDocumentStore store, string id, RavenServer server, int bucket, int curShard, int toShard)
        {
            if (server == null)
            {
                server = _parent.Server;
            }

            var sourceName = ShardHelper.ToShardName(store.Database, curShard);
            var destName = ShardHelper.ToShardName(store.Database, toShard);

            using (var session = store.OpenAsyncSession(sourceName))
            {
                var user = await session.LoadAsync<dynamic>(id);
                Assert.NotNull(user);
            }

            //var result = await server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());
            var cmd = new StartBucketMigrationCommand(bucket, curShard, toShard, store.Database, RaftIdGenerator.NewId());
            var result = await server.ServerStore.SendToLeaderAsync(cmd);
            var migrationIndex = result.Index;

            var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: destName);
            Assert.True(exists);

            string changeVector;
            using (var session = store.OpenAsyncSession(destName))
            {
                var doc = await session.LoadAsync<dynamic>(id);
                changeVector = session.Advanced.GetChangeVectorFor(doc);
                Assert.False(string.IsNullOrEmpty(changeVector), "changeVector");
            }

            var res1 = await server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, migrationIndex, changeVector, RaftIdGenerator.NewId());
            await server.ServerStore.Cluster.WaitForIndexNotification(res1.Index);

            // here I need to create a doc ? (so I will have 2 batches in delete)
            _forTestingPurposes?.BetweenSourceAndDestinationMigrationCompletion?.Invoke();
            // here maybe disable EnableWritesToTheWrongShard

            var res2 = await server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, migrationIndex);
            await server.ServerStore.Cluster.WaitForIndexNotification(res2.Index);

            var shardDb = await _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(store).Where(x => x.ShardNumber == curShard).FirstOrDefaultAsync();
            await shardDb.DeleteBucketAsync(bucket, migrationIndex, changeVector);

            await WaitForMigrationComplete(store, bucket);

            using (var session = store.OpenAsyncSession(destName))
            {
                var doc = await session.LoadAsync<dynamic>(id);
                Assert.NotNull(doc);
            }
        }

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action BetweenSourceAndDestinationMigrationCompletion;

            internal IDisposable CallBetweenSourceAndDestinationMigrationCompletion(Action action)
            {
                BetweenSourceAndDestinationMigrationCompletion = action;

                return new DisposableAction(() => BetweenSourceAndDestinationMigrationCompletion = null);
            }
        }
    }
}

﻿using System;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide.Sharding;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class BucketMigrationReplication : ReplicationNode
    {
        public readonly int Bucket;
        public readonly int Shard;
        public readonly string Node;
        public readonly long MigrationIndex;

        public BucketMigrationReplication(int bucket, int destShard, string destResponsibleNode, long migrationIndex)
        {
            Node = destResponsibleNode ?? throw new ArgumentNullException(nameof(destResponsibleNode));
            Bucket = bucket;
            Shard = destShard;
            MigrationIndex = migrationIndex;
        }

        public bool ForBucketMigration(ShardBucketMigration migration)
        {
            if (migration.MigrationIndex != MigrationIndex)
                return false;

            if (migration.Bucket != Bucket)
                return false;

            if (migration.DestinationShard != Shard)
                return false;

            return true;
        }

        public override int GetHashCode() => (int)(CalculateStringHash(Node) ^ (ulong)Hashing.Mix(MigrationIndex));

        public override string FromString() => $"Migrating bucket '{Bucket}' to shard '{Shard}' on node '{Node}' @ {MigrationIndex}";

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Bucket)] = Bucket;
            json[nameof(Shard)] = Shard;
            json[nameof(MigrationIndex)] = MigrationIndex;
            json[nameof(Node)] = Node;
            return json;
        }
    }
}

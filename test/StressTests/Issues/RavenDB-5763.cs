﻿using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Replication;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_5763 : NoDisposalNoOutputNeeded
    {
        public RavenDB_5763(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_not_throw_timeout_and_out_of_memory()
        {
            Parallel.For(0, 3, RavenTestHelper.DefaultParallelOptions, async _ =>
            {
                using (var store = new ReplicationTombstoneTests(Output))
                {
                    await store.Two_tombstones_should_replicate_in_master_master();
                }
            });
        }
    }
}

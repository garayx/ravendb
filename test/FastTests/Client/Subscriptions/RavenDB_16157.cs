using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_16157 : ReplicationTestBase
    {
        public RavenDB_16157(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Count { get; set; }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscriptions_WithFilterOnRefreshFieldOnMetadata()
        {
            using (var store = GetDocumentStore())
            {
                var ops1 = new SubscriptionCreationOptions<User>
                {
                    Filter = user => user.Count > 0 && RavenQuery.Metadata(user).ContainsKey(Constants.Documents.Metadata.Refresh) == false
                };
                var subId = await store.Subscriptions.CreateAsync(ops1);

                using (var session = store.OpenAsyncSession())
                {
                    var u1 = new User { Count = 1, Name = "user1" };
                    await session.StoreAsync(u1);
                    var metadata = session.Advanced.GetMetadataFor(u1);
                    metadata[Constants.Documents.Metadata.Refresh] = DateTime.UtcNow.AddMinutes(5);

                    var u2 = new User { Count = 1, Name = "user2" };
                    await session.StoreAsync(u2);

                    await session.SaveChangesAsync();
                }

                await using var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subId);

                var users = new List<User>();
                var mre = new ManualResetEventSlim();

                var processingTask = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        users.Add(item.Result);
                    }
                    mre.Set();
                });

                Assert.True(mre.Wait(TimeSpan.FromSeconds(30)));

                Assert.Equal(1, users.Count);
                Assert.Equal("user2", users[0].Name);

                await subscription.DisposeAsync();
                await processingTask;
            }
        }
    }
}

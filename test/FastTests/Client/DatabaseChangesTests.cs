﻿using Tests.Infrastructure;
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class DatabaseChangesTests : ClusterTestBase
    {
        public DatabaseChangesTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DatabaseChanges_WhenRetryAfterCreatingDatabase_ShouldSubscribe(bool disableTopologyUpdates)
        {
            var database = GetDatabaseName();
            var server = GetNewServer();
            using var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { server.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = disableTopologyUpdates }
            }.Initialize();

            await server.ServerStore.EnsureNotPassiveAsync();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                try
                {
                    await obs.EnsureSubscribedNow();
                }
                catch (DatabaseDoesNotExistException)
                {
                    //ignore
                }
                catch (AggregateException e) when (e.InnerException is DatabaseDoesNotExistException)
                {
                    //ignore
                }
            }

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database))).ConfigureAwait(false);
            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DatabaseChanges_WhenTryToReconnectAfterDeletingDatabase_ShouldFailToSubscribe(bool disableTopologyUpdates)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Conventions = new DocumentConventions { DisableTopologyUpdates = disableTopologyUpdates }
            });

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();
            }

            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);
            using (var changes = store.Changes())
            {
                var message = string.Empty;
                changes.OnError += exception => Volatile.Write(ref message, exception.Message);
                var obs = changes.ForDocumentsInCollection<User>();
                var task = obs.EnsureSubscribedNow();
                var timeout = TimeSpan.FromSeconds(30);
                if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
                    throw new TimeoutException($"{timeout} {message}");

                var e = await Assert.ThrowsAnyAsync<Exception>(() => task);
                if (e is AggregateException ae)
                    e = ae.ExtractSingleInnerException();
                
                Assert.Equal(typeof(DatabaseDoesNotExistException), e.GetType());
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DatabaseChanges_WhenDeleteDatabaseAfterSubscribe_ShouldSetConnectionStateToDatabaseDoesNotExistException(bool disableTopologyUpdates)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Conventions = new DocumentConventions { DisableTopologyUpdates = disableTopologyUpdates }
            });

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);

                await AssertWaitForExceptionAsync<DatabaseDoesNotExistException>(async () => await obs.EnsureSubscribedNow(), interval: 1000);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DatabaseChanges_WhenDisposeDatabaseChanges_ShouldSetConnectionStateDisposed(bool disableTopologyUpdates)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Conventions = new DocumentConventions { DisableTopologyUpdates = disableTopologyUpdates }
            });

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();

                changes.Dispose();
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await obs.EnsureSubscribedNow());
            }
        }
    }
}

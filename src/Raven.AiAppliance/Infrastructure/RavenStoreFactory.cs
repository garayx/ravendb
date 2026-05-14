using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Raven.AiAppliance.Infrastructure;

public static class RavenStoreFactory
{
    public static IDocumentStore Create(ApplianceOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.RavenUrl))
            throw new ArgumentException("RavenUrl must not be empty.", nameof(options));
        if (string.IsNullOrWhiteSpace(options.ConfigDatabase))
            throw new ArgumentException("ConfigDatabase must not be empty.", nameof(options));

        var store = new DocumentStore
        {
            Urls = [options.RavenUrl],
            Database = options.ConfigDatabase,
        };
        store.Initialize();
        return store;
    }

    public static IDocumentStore Create(IOptions<ApplianceOptions> options) =>
        Create(options.Value);

    public static async Task<bool> EnsureDatabaseAsync(IDocumentStore store, string database, CancellationToken ct = default)
    {
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct);
        if (record is not null)
            return false;

        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)), ct);
        return true;
    }
}

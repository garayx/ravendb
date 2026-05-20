using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.AiAppliance.Endpoints;

/// Stage C.1 wizard backend endpoints. Mapped unconditionally — there is no
/// endpoint-level <see cref="BootstrapPhase"/> gate yet, so callers must
/// respect <c>/api/bootstrap/status</c> and only POST here once the appliance
/// reports <c>Ready</c>. A middleware-based gate that returns 503 for
/// non-bootstrap routes while <see cref="BootstrapPhase"/> != Ready is a
/// follow-up.
public static class WizardEndpoints
{
    /// Fixed-name connection string used by the wizard to probe a source DB
    /// before any per-app configuration exists. Lives on the config DB; gets
    /// overwritten on each Connect call.
    private const string WizardSourceProbeName = "_wizard-source-probe";

    private static readonly HashSet<string> SupportedProviders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Npgsql",
        "System.Data.SqlClient",
        "MySql.Data.MySqlClient",
    };

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/setup");
        group.MapPost("/connect",  ConnectAsync);
        group.MapPost("/discover", DiscoverAsync);
    }

    private static async Task<IResult> ConnectAsync(
        ConnectRequest body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body, out var error))
            return error;

        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        var sqlConnectionString = new SqlConnectionString
        {
            Name             = WizardSourceProbeName,
            FactoryName      = body!.Provider,
            ConnectionString = body.ConnectionString,
        };
        await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString), ct);

        ConnectResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
                new CdcSinkVerifyOperation(WizardSourceProbeName, body.TableNames), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Connect: verify threw");
            result = new ConnectResult();
            result.Errors.Add($"Verification threw: {ex.Message}");
        }

        await PersistAsync(store, opts.ConfigDatabase, state =>
        {
            state.Provider         = body.Provider;
            state.ConnectionString = body.ConnectionString;
            state.LastVerifyResult = result;
            state.LastVerifyAt     = DateTime.UtcNow;
        }, ct);

        return Results.Ok(result);
    }

    private static async Task<IResult> DiscoverAsync(
        ConnectRequest body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body, out var error))
            return error;

        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        CdcSinkSourceSchema schema;
        try
        {
            schema = await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
                new GetCdcSinkSchemaOperation(
                    new SqlConnectionString
                    {
                        Name             = WizardSourceProbeName,
                        FactoryName      = body!.Provider,
                        ConnectionString = body.ConnectionString,
                    }),
                ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Discover: schema enumeration threw");
            schema = new CdcSinkSourceSchema();
            schema.Errors.Add($"Discovery threw: {ex.Message}");
        }

        await PersistAsync(store, opts.ConfigDatabase, state =>
        {
            state.Provider             = body.Provider;
            state.ConnectionString     = body.ConnectionString;
            state.LastDiscoveredSchema = schema;
            state.LastDiscoverAt       = DateTime.UtcNow;
        }, ct);

        return Results.Ok(schema);
    }

    private static bool TryRejectInvalidRequest(ConnectRequest? body, out IResult error)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.Provider) || string.IsNullOrWhiteSpace(body.ConnectionString))
        {
            error = Results.BadRequest(new { error = "provider and connectionString are required" });
            return true;
        }

        if (!SupportedProviders.Contains(body.Provider))
        {
            error = Results.BadRequest(new
            {
                error = $"unsupported provider '{body.Provider}'. Supported: {string.Join(", ", SupportedProviders)}",
            });
            return true;
        }

        error = default!;
        return false;
    }

    private static async Task PersistAsync(
        IDocumentStore store,
        string configDb,
        Action<WizardState> mutate,
        CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(configDb);
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct)
                    ?? new WizardState();
        mutate(state);
        await session.StoreAsync(state, WizardState.DocumentId, ct);
        await session.SaveChangesAsync(ct);
    }

    /// Logger category marker.
    public sealed class WizardLogger;
}

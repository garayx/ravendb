using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
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

    /// <summary>
    /// CDC-supported subset of <see cref="SqlProvider"/>. The CDC server-side
    /// (<c>CdcSinkSchemaDiscovery</c>) accepts only these three enum values;
    /// the rest of <see cref="SqlProvider"/> (Oracle / OleDb / SqlServerCe) is
    /// recognized by Raven.Client but not supported by CDC. Kept here instead
    /// of duplicating factory-name strings — <see cref="SqlProviderParser"/>
    /// already owns the string-to-enum mapping.
    /// </summary>
    private static readonly HashSet<SqlProvider> CdcSupportedProviders = new()
    {
        SqlProvider.Npgsql,
        SqlProvider.SqlClient,
        SqlProvider.MySqlConnectorFactory,
    };

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/setup");
        group.MapPost("/connect",      ConnectAsync);
        group.MapPost("/discover",     DiscoverAsync);
        group.MapPost("/map",          MapAsync);
        group.MapPost("/test-mapping", TestMappingAsync);
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
            // Don't persist the raw ConnectionString — credentials are kept
            // only on the registered _wizard-source-probe SqlConnectionString
            // (one source of truth) to minimise exposure.
            state.Provider         = body.Provider;
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

        // Upsert the probe (idempotent — same shape Connect uses) so the
        // schema-discovery call can use the *named* SqlConnectionString
        // overload. That avoids sending the raw credentials inline a second
        // time, and guarantees Discover uses the same connection string the
        // probe is registered with (no drift if the caller passes different
        // bytes to Connect vs Discover).
        var sqlConnectionString = new SqlConnectionString
        {
            Name             = WizardSourceProbeName,
            FactoryName      = body!.Provider,
            ConnectionString = body.ConnectionString,
        };
        await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString), ct);

        CdcSinkSourceSchema schema;
        try
        {
            schema = await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
                new GetCdcSinkSchemaOperation(WizardSourceProbeName), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Discover: schema enumeration threw");
            schema = new CdcSinkSourceSchema();
            schema.Errors.Add($"Discovery threw: {ex.Message}");
        }

        await PersistAsync(store, opts.ConfigDatabase, state =>
        {
            // ConnectionString deliberately not persisted — see Connect handler note.
            state.Provider             = body.Provider;
            state.LastDiscoveredSchema = schema;
            state.LastDiscoverAt       = DateTime.UtcNow;
        }, ct);

        return Results.Ok(schema);
    }

    /// <summary>
    /// W3 Map. Accepts a CdcSinkConfiguration JSON (manual or import path),
    /// applies forgiving defaults for the wizard-context fields (Name +
    /// ConnectionStringName), validates it, and persists to wizard-state for
    /// W4 Test-mapping / W6 Provision to read back. No LLM path (AI-suggest)
    /// in this slice.
    /// </summary>
    private static async Task<IResult> MapAsync(
        CdcSinkConfiguration body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new { error = "request body required" });

        // Forgiving defaults — caller supplies Tables, wizard fills the
        // scaffolding. Provision (W6) renames `Name` to the per-app value;
        // ConnectionStringName defaults to the probe Connect/Discover
        // already register on the config DB.
        if (string.IsNullOrWhiteSpace(body.Name))
            body.Name = "wizard-cdc";
        if (string.IsNullOrWhiteSpace(body.ConnectionStringName))
            body.ConnectionStringName = WizardSourceProbeName;

        // validateConnection: false — Map doesn't bind a real SqlConnectionString
        // to the config object. The probe is on the config DB; the per-app CS is
        // set up at Provision time.
        if (!body.Validate(out var errors, validateName: true, validateConnection: false))
        {
            logger.LogInformation("Map: configuration rejected by Validate ({Count} errors)", errors.Count);
            return Results.BadRequest(new { errors });
        }

        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);
        await PersistAsync(store, opts.ConfigDatabase, state =>
        {
            state.LastMapConfiguration = body;
            state.LastMapAt            = DateTime.UtcNow;
        }, ct);

        return Results.Ok(body);
    }

    /// <summary>
    /// W4 Test-mapping. Reads wizard-state.LastMapConfiguration back, builds a
    /// TestCdcSinkMappingRequest with sane defaults, and forwards to the
    /// server's /admin/cdc-sink/test endpoint via TestCdcSinkMappingOperation
    /// (internal in Raven.Client; reachable here via InternalsVisibleTo). The
    /// server resolves the source-DB credentials by name from the registered
    /// _wizard-source-probe SqlConnectionString — no credentials re-sent.
    /// </summary>
    private static async Task<IResult> TestMappingAsync(
        TestMappingRequest body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        ILogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.SourceTableName))
            return Results.BadRequest(new { error = "sourceTableName is required" });

        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        WizardState? state;
        using (var session = store.OpenAsyncSession(opts.ConfigDatabase))
            state = await session.LoadAsync<WizardState>(WizardState.DocumentId, ct);

        if (state?.LastMapConfiguration is null)
            return Results.BadRequest(new { error = "no map configuration found; call /api/setup/map first" });

        var request = new TestCdcSinkMappingRequest
        {
            Configuration     = state.LastMapConfiguration,
            SourceTableSchema = body.SourceTableSchema,
            SourceTableName   = body.SourceTableName,
            RowSelector       = TestCdcSinkRowSelector.First,
            Operation         = TestCdcSinkOperation.Upsert,
            MaxRows           = body.MaxRows ?? 50,
        };

        TestCdcSinkMappingResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(opts.ConfigDatabase).SendAsync(
                new TestCdcSinkMappingOperation(request), ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "TestMapping: SendAsync threw");
            result = new TestCdcSinkMappingResult();
            result.Errors.Add($"Test mapping threw: {ex.Message}");
        }

        return Results.Ok(result);
    }

    private static bool TryRejectInvalidRequest(ConnectRequest? body, out IResult error)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.Provider) || string.IsNullOrWhiteSpace(body.ConnectionString))
        {
            error = Results.BadRequest(new { error = "provider and connectionString are required" });
            return true;
        }

        // Resolve the factory-name string via Raven.Client's canonical parser
        // (covers both legacy + modern driver names, e.g. System.Data.SqlClient
        // *and* Microsoft.Data.SqlClient both -> SqlProvider.SqlClient). Then
        // narrow to the CDC-supported enum subset — Oracle/OleDb/SqlServerCe
        // are valid SqlProvider values but the CDC sink doesn't support them.
        SqlProvider parsed;
        try
        {
            parsed = SqlProviderParser.GetSupportedProvider(body.Provider);
        }
        catch (Exception ex) when (ex is NotSupportedException or NotImplementedException)
        {
            error = Results.BadRequest(new { error = $"unsupported provider '{body.Provider}': {ex.Message}" });
            return true;
        }

        if (!CdcSupportedProviders.Contains(parsed))
        {
            error = Results.BadRequest(new
            {
                error = $"provider '{body.Provider}' (parses as {parsed}) is recognized by Raven.Client but not supported by CDC. Supported: {string.Join(", ", CdcSupportedProviders)}.",
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
    internal sealed class WizardLogger;
}

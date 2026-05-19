using System.IO.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Endpoints;

/// First-run flow endpoints. Live regardless of <see cref="IBootstrapState"/> —
/// they're the only way out of <see cref="BootstrapPhase.NeedsActivation"/>.
public static class BootstrapEndpoints
{
    public sealed record RedeemLicenseRequest(string LicenseKey);

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/bootstrap");
        group.MapGet("/status", GetStatus);
        group.MapPost("/redeem-license", RedeemLicenseAsync);
    }

    private static IResult GetStatus(IBootstrapState state) =>
        Results.Ok(new
        {
            state = PhaseToWire(state.Phase),
            reason = state.Reason,
        });

    /// <summary>
    /// First-run activation. Fetches the setup-package zip from the configured
    /// license upstream and unpacks it into <see cref="ApplianceOptions.SetupPackagePath"/>.
    /// Production-side cert reload + RavenDB restart is a follow-up; for now we
    /// flip <see cref="IBootstrapState"/> to Ready once the package is on disk
    /// so the wizard endpoints become live. Tests inject the appliance's
    /// IDocumentStore separately, so no in-process RavenDB restart is needed
    /// for the E2E happy path.
    /// </summary>
    private static async Task<IResult> RedeemLicenseAsync(
        RedeemLicenseRequest body,
        IBootstrapState bootstrap,
        IOptions<ApplianceOptions> options,
        IHttpClientFactory httpClientFactory,
        ILogger<BootstrapLicenseLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.LicenseKey))
            return Results.BadRequest(new { error = "licenseKey is required" });

        var opts = options.Value;
        bootstrap.MarkRedeeming();

        try
        {
            using var http = httpClientFactory.CreateClient();
            var url = $"{opts.LicenseApiUrl.TrimEnd('/')}/licenses/{Uri.EscapeDataString(body.LicenseKey)}";
            using var upstream = await http.GetAsync(url, ct);
            if (!upstream.IsSuccessStatusCode)
            {
                var msg = $"license api returned {(int)upstream.StatusCode} {upstream.ReasonPhrase}";
                logger.LogWarning("License redemption failed: {Reason}", msg);
                bootstrap.MarkFailed(msg);
                return Results.Problem(detail: msg, statusCode: (int)upstream.StatusCode);
            }

            var zipBytes = await upstream.Content.ReadAsByteArrayAsync(ct);

            Directory.CreateDirectory(opts.SetupPackagePath);
            using (var ms = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(ms, ZipArchiveMode.Read))
            {
                archive.ExtractToDirectory(opts.SetupPackagePath, overwriteFiles: true);
            }

            logger.LogInformation(
                "Setup package redeemed and unpacked to {Path} ({Bytes} bytes).",
                opts.SetupPackagePath, zipBytes.Length);

            bootstrap.MarkReady();
            return Results.Ok(new { state = "ready" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            bootstrap.MarkFailed("redemption cancelled");
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "License redemption threw.");
            bootstrap.MarkFailed(ex.Message);
            return Results.Problem(detail: ex.Message, statusCode: 500);
        }
    }

    private static string PhaseToWire(BootstrapPhase phase) => phase switch
    {
        BootstrapPhase.NeedsActivation => "needs-activation",
        BootstrapPhase.Redeeming       => "redeeming",
        BootstrapPhase.Ready           => "ready",
        _ => phase.ToString().ToLowerInvariant(),
    };

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    public sealed class BootstrapLicenseLogger;
}

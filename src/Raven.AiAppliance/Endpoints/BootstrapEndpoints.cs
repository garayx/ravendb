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
            state = state.Phase.ToWire(),
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

        // CAS guard against operator double-click — two concurrent POSTs
        // would otherwise both fetch + extract into /setup/, interleaving
        // zips and racing the IDocumentStore reload.
        if (!bootstrap.TryMarkRedeeming())
        {
            return Results.Conflict(new
            {
                error = "redemption already in progress or completed",
                state = bootstrap.Phase.ToWire(),
            });
        }

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

            // Stream the zip to a temp file (capped) instead of buffering in
            // memory — a misbehaving / malicious upstream returning a huge
            // archive would otherwise OOM the appliance. The cap also bounds
            // disk usage. Real production setup packages are <100 KB; a 32 MB
            // cap gives 300× headroom.
            const long MaxSetupPackageBytes = 32L * 1024 * 1024;
            var tempZipPath = Path.Combine(Path.GetTempPath(), $"setup-package-{Guid.NewGuid():N}.zip");
            long downloadedBytes;
            try
            {
                await using (var upstreamStream = await upstream.Content.ReadAsStreamAsync(ct))
                await using (var tempFile = File.Create(tempZipPath))
                {
                    var buffer = new byte[81920];
                    int read;
                    while ((read = await upstreamStream.ReadAsync(buffer, ct)) > 0)
                    {
                        if (tempFile.Position + read > MaxSetupPackageBytes)
                            throw new InvalidOperationException(
                                $"setup package exceeds the {MaxSetupPackageBytes:N0} byte cap; aborting download.");
                        await tempFile.WriteAsync(buffer.AsMemory(0, read), ct);
                    }
                    downloadedBytes = tempFile.Position;
                }

                Directory.CreateDirectory(opts.SetupPackagePath);
                // Zip Slip protection: ZipFile.ExtractToDirectory in .NET 9+
                // (we target net10.0) resolves each entry's destination via
                // Path.GetFullPath against the target dir and throws IOException
                // if the resolved path escapes the destination — so `../` and
                // absolute-path entries from a hostile / corrupted zip are
                // rejected before any file is written. No manual entry-name
                // validation needed.
                ZipFile.ExtractToDirectory(tempZipPath, opts.SetupPackagePath, overwriteFiles: true);
            }
            finally
            {
                // Best-effort cleanup. Narrow to the two exceptions File.Delete
                // can legitimately throw on a stray temp file (the path is
                // ours, no malformed-path risks) — anything else is unexpected
                // and worth letting bubble.
                try { File.Delete(tempZipPath); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    logger.LogDebug(ex, "Failed to delete temp zip {Path}", tempZipPath);
                }
            }

            logger.LogInformation(
                "Setup package redeemed and unpacked to {Path} ({Bytes} bytes).",
                opts.SetupPackagePath, downloadedBytes);

            bootstrap.MarkReady();
            return Results.Ok(new { state = "ready" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            bootstrap.MarkFailed("redemption cancelled");
            throw;
        }
        catch (InvalidDataException ex)
        {
            // ZipFile.ExtractToDirectory throws InvalidDataException for a
            // corrupt / non-zip payload. That's an upstream-bad-bytes problem,
            // not an appliance failure — surface as 502 Bad Gateway so the
            // caller can distinguish "license server returned garbage" from
            // "the appliance itself is broken".
            var detail = $"invalid setup package: {ex.Message}";
            logger.LogWarning(ex, "License redemption: upstream returned an invalid zip.");
            bootstrap.MarkFailed(detail);
            return Results.Problem(detail: detail, statusCode: StatusCodes.Status502BadGateway);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "License redemption threw.");
            bootstrap.MarkFailed(ex.Message);
            return Results.Problem(detail: ex.Message, statusCode: 500);
        }
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class BootstrapLicenseLogger;
}

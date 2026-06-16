using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.AiHelper;

namespace AiApplianceTests.E2E.Fixtures;

/// In-process stand-in for the api.ravendb.net "Quill" license endpoint (PR #3003).
/// Hosts GET /api/v1/quill/licenses/{key} returning { license, domain } when {key} matches
/// the configured token, 404 otherwise. Caller disposes; the bound base URL is exposed for the
/// appliance to dial.
public sealed class MockQuillApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    private MockQuillApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public static async Task<MockQuillApi> StartAsync(string token, ApplianceLicense license, string domain)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        app.MapGet("/api/v1/quill/licenses/{key}", (string key) =>
            string.Equals(key, token, StringComparison.Ordinal)
                ? Results.Json(new { license, domain })
                : Results.NotFound());

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockQuillApi failed to bind a port.");

        return new MockQuillApi(app, url.TrimEnd('/'));
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

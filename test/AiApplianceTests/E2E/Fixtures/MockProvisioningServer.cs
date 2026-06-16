using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AiApplianceTests.E2E.Fixtures;

/// In-process stand-in for RavenDB's POST /setup/appliance/provision endpoint. Captures the
/// last request body (so tests can assert the license + domain were posted) and returns the
/// configured setup-package zip bytes, or a configured non-200 status. Mirrors
/// <see cref="MockAiApi"/>. The caller disposes; the bound base URL is exposed for the
/// appliance via ApplianceOptions.RavenUrl.
public sealed class MockProvisioningServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    /// Last raw request body received on /setup/appliance/provision.
    public string? LastRequestBody { get; private set; }

    private MockProvisioningServer(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public static async Task<MockProvisioningServer> StartAsync(byte[] zipBytes, int statusCode = 200)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        // Late-bound holder so the route handler can reach the instance created after
        // the app is built (same shape as MockAiApi's closure).
        MockProvisioningServer instance = null!;

        app.MapPost("/setup/appliance/provision", async (HttpContext ctx) =>
        {
            using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
            instance.LastRequestBody = await reader.ReadToEndAsync();

            return statusCode != 200
                ? Results.StatusCode(statusCode)
                : Results.File(zipBytes, "application/zip");
        });

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockProvisioningServer failed to bind a port.");

        instance = new MockProvisioningServer(app, url.TrimEnd('/'));
        return instance;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

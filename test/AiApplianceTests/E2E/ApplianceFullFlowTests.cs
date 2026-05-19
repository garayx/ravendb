using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests.E2E;

/// End-to-end happy path for the AI Appliance. Drives the full feature flow:
/// license redemption -> CDC wizard (Connect / Discover / Map / Test / Provision)
/// -> initial load -> AI agent + iFrame channel. Written upfront with all 13 step
/// assertions present; goes RED at the first unimplemented step. Each slice in
/// the plan's "Roadmap to E2E GREEN" turns one more assertion GREEN.
///
/// Prereqs:
///   - RAVEN_NPGSQL_CONNECTION_STRING env var pointing at a Postgres the test
///     infrastructure can create + drop databases on.
///   - *.egor-ai.ravendb.run DNS -> 127.0.0.1 (the wildcard cert from the
///     embedded setup-package zip is for that domain).
///
/// Optional: set APPLIANCE_E2E_HOLD=1 to park the test after T12 so you can
/// open the live iFrame in a browser.
public class ApplianceFullFlowTests(ITestOutputHelper output) : CdcSinkIntegrationTestBase(output)
{
    private const string HardcodedLicenseKey = "egor-ai-test-license";

    [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
    public async Task EndToEnd_FullApplianceFlow_PostgresSourceToIFrameAgent_Works()
    {
        // ---------- T1. Mock license API serving the real setup-package zip ----------
        var zipPath = Path.Combine(AppContext.BaseDirectory, "egor-ai.Cluster.Settings.zip");
        Assert.True(File.Exists(zipPath), $"Setup-package zip fixture missing at {zipPath}");
        var zipBytes = await File.ReadAllBytesAsync(zipPath);

        await using var licenseApi = await MockLicenseApi.StartAsync(HardcodedLicenseKey, zipBytes);
        var setupRoot = NewDataPath(forceCreateDir: true, prefix: "egor-ai-setup");

        // ---------- T2. Appliance starts in NEEDS-ACTIVATION ----------
        using var store = GetDocumentStore();
        using var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: licenseApi.BaseAddress,
            setupPackagePath: setupRoot,
            applianceStore: store);
        var client = factory.CreateClient();

        var statusBefore = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
        Assert.Equal("needs-activation", statusBefore.GetProperty("state").GetString());

        var healthBefore = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, healthBefore.StatusCode);

        // ---------- T3. Redeem license, wait for READY ----------
        var redeem = await client.PostAsJsonAsync("/api/bootstrap/redeem-license",
            new { licenseKey = HardcodedLicenseKey });
        Assert.True(redeem.IsSuccessStatusCode,
            $"redeem returned {redeem.StatusCode}: {await redeem.Content.ReadAsStringAsync()}");

        await WaitForBootstrapStateAsync(client, expected: "ready", timeoutMs: 60_000);

        var healthAfter = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, healthAfter.StatusCode);

        // ---------- T4. Source Postgres with Northwind data ----------
        using var sqlTeardown = WithSqlDatabase(MigrationProvider.NpgSQL,
            out var pgConnStr, out _, dataSet: "northwind", includeData: true);

        // ---------- T5. Connect (CDC verify) ----------
        var connectResp = await client.PostAsJsonAsync("/api/setup/connect",
            new { provider = "Npgsql", connectionString = pgConnStr });
        Assert.True(connectResp.IsSuccessStatusCode,
            $"connect returned {connectResp.StatusCode}: {await connectResp.Content.ReadAsStringAsync()}");
        var verify = await connectResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(verify.GetProperty("success").GetBoolean(),
            $"verify should succeed; payload: {verify}");

        // ---------- T6. Discover schema ----------
        var discoverResp = await client.PostAsJsonAsync("/api/setup/discover",
            new { provider = "Npgsql", connectionString = pgConnStr });
        Assert.True(discoverResp.IsSuccessStatusCode,
            $"discover returned {discoverResp.StatusCode}: {await discoverResp.Content.ReadAsStringAsync()}");
        var schema = await discoverResp.Content.ReadFromJsonAsync<JsonElement>();

        var tableNames = schema.GetProperty("tables").EnumerateArray()
            .Select(t => t.GetProperty("sourceTableName").GetString()!.ToLowerInvariant())
            .ToHashSet();
        Assert.Contains("orders", tableNames);
        Assert.Contains("customers", tableNames);
        Assert.Contains("products", tableNames);

        // ---------- T7. Map: POST a pre-built CdcSinkConfiguration for Northwind ----------
        var configFixturePath = Path.Combine(AppContext.BaseDirectory, "E2E", "Fixtures", "northwind-cdc-config.json");
        Assert.True(File.Exists(configFixturePath),
            $"Pre-built CDC config fixture missing at {configFixturePath}. Populated by the W3 Map slice.");
        var configJson = await File.ReadAllTextAsync(configFixturePath);

        var mapResp = await client.PostAsync("/api/setup/map",
            new StringContent(configJson, Encoding.UTF8, "application/json"));
        Assert.True(mapResp.IsSuccessStatusCode,
            $"map returned {mapResp.StatusCode}: {await mapResp.Content.ReadAsStringAsync()}");

        // ---------- T8. Test-mapping ----------
        var testResp = await client.PostAsJsonAsync("/api/setup/test-mapping",
            new { sourceTableName = "orders", maxRows = 50 });
        Assert.True(testResp.IsSuccessStatusCode,
            $"test-mapping returned {testResp.StatusCode}: {await testResp.Content.ReadAsStringAsync()}");
        var testJson = await testResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(testJson.GetProperty("rows").GetArrayLength() > 0, "expected non-empty test-mapping result");

        // ---------- T9. Provision ----------
        var provisionResp = await client.PostAsJsonAsync("/api/setup/provision",
            new { appName = "northwind-demo" });
        Assert.True(provisionResp.IsSuccessStatusCode,
            $"provision returned {provisionResp.StatusCode}: {await provisionResp.Content.ReadAsStringAsync()}");
        var provisionJson = await provisionResp.Content.ReadFromJsonAsync<JsonElement>();
        var appId = provisionJson.GetProperty("appId").GetString();
        Assert.False(string.IsNullOrEmpty(appId));

        // ---------- T10. Wait for initial load ----------
        await WaitForCdcInitialLoadAsync(store, "northwind-demo-cdc", timeoutMs: 120_000);
        var ordersCount = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 800, timeoutMs: 30_000);
        Assert.True(ordersCount >= 800,
            $"expected >=800 Orders after initial load, got {ordersCount}");

        // ---------- T11. AI agent ----------
        var agentResp = await client.PostAsJsonAsync($"/api/apps/{appId}/setup/agent",
            new { framing = "customer-support" });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");
        var agentJson = await agentResp.Content.ReadFromJsonAsync<JsonElement>();
        var agentId = agentJson.GetProperty("agentId").GetString();
        Assert.False(string.IsNullOrEmpty(agentId));

        // ---------- T12. iFrame channel ----------
        var channelResp = await client.PostAsJsonAsync($"/api/apps/{appId}/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });
        Assert.True(channelResp.IsSuccessStatusCode,
            $"channel returned {channelResp.StatusCode}: {await channelResp.Content.ReadAsStringAsync()}");
        var channelJson = await channelResp.Content.ReadFromJsonAsync<JsonElement>();
        var widgetId = channelJson.GetProperty("widgetId").GetString();
        Assert.False(string.IsNullOrEmpty(widgetId));

        // ---------- T13. Optional manual park ----------
        if (Environment.GetEnvironmentVariable("APPLIANCE_E2E_HOLD") == "1")
        {
            Console.WriteLine($"Embed URL: {client.BaseAddress}embed/{widgetId}");
            Console.WriteLine("Test parked. Ctrl+C to exit.");
            await Task.Delay(Timeout.Infinite);
        }
    }

    private static async Task WaitForBootstrapStateAsync(HttpClient client, string expected, int timeoutMs)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            try
            {
                var status = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
                if (status.GetProperty("state").GetString() == expected)
                    return;
            }
            catch (Exception)
            {
                // status endpoint may be momentarily unavailable mid-bootstrap; keep polling.
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"BootstrapState did not reach '{expected}' within {timeoutMs}ms.");
    }
}

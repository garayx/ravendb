using System.IO.Compression;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Bootstrap;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// Drives POST /api/bootstrap/redeem-license through the new resolve+provision seams with
/// fakes: no real license API, no real RavenDB provisioning, no s6. Asserts the redeem flow
/// resolves the token, provisions a package, extracts it to the setup-package dir, and reaches
/// Ready (the inline branch taken when RavenDbS6Service is empty).
public class BootstrapRedeemEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Redeem_resolves_token_provisions_package_extracts_and_reaches_ready()
    {
        var store = GetDocumentStore();
        var setupPath = NewDataPath(forceCreateDir: true);

        var resolver = new RecordingResolver(new LicenseAndDomain(
            new ApplianceLicense { Id = "abc", Name = "test", Keys = ["k1"] }, "egor-ai"));
        var provisioner = new RecordingProvisioner(BuildPackageZip());

        using var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: setupPath,
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database,
            configureServices: services =>
            {
                services.RemoveAll<ILicenseDomainResolver>();
                services.AddSingleton<ILicenseDomainResolver>(resolver);
                services.RemoveAll<ISetupPackageProvisioner>();
                services.AddSingleton<ISetupPackageProvisioner>(provisioner);
            });
        var client = factory.CreateClient();

        var statusBefore = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
        Assert.Equal("NeedsActivation", statusBefore.GetProperty("state").GetString());

        var redeem = await client.PostAsJsonAsync("/api/bootstrap/redeem-license", new { licenseKey = "quill" });
        Assert.True(redeem.IsSuccessStatusCode,
            $"redeem returned {redeem.StatusCode}: {await redeem.Content.ReadAsStringAsync()}");

        var statusAfter = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
        Assert.Equal("Ready", statusAfter.GetProperty("state").GetString());

        // The token was resolved and the resolved {license, domain} flowed into provisioning.
        Assert.Equal("quill", resolver.LastToken);
        Assert.Equal("egor-ai", provisioner.LastInput?.Domain);
        Assert.Equal("abc", provisioner.LastInput?.License.Id);

        // The provisioned package was unpacked into the setup-package dir.
        Assert.True(File.Exists(Path.Combine(setupPath, "A", "settings.json")),
            "expected the provisioned package to be extracted to <setup>/A/settings.json");
    }

    private static byte[] BuildPackageZip()
    {
        using var ms = new MemoryStream();
        using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            var entry = zip.CreateEntry("A/settings.json");
            using var w = new StreamWriter(entry.Open(), Encoding.UTF8);
            w.Write("""{"PublicServerUrl":"https://a.egor-ai.ravendb.run"}""");
        }
        return ms.ToArray();
    }

    private sealed class RecordingResolver(LicenseAndDomain result) : ILicenseDomainResolver
    {
        public string? LastToken { get; private set; }

        public Task<LicenseAndDomain> ResolveAsync(string token, CancellationToken ct)
        {
            LastToken = token;
            return Task.FromResult(result);
        }
    }

    private sealed class RecordingProvisioner(byte[] zip) : ISetupPackageProvisioner
    {
        public LicenseAndDomain? LastInput { get; private set; }

        public Task<Stream> ProvisionAsync(LicenseAndDomain licenseAndDomain, CancellationToken ct)
        {
            LastInput = licenseAndDomain;
            return Task.FromResult<Stream>(new MemoryStream(zip));
        }
    }
}

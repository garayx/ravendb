using System.Net;
using System.Net.Http;
using System.Text.Json.Nodes;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Bootstrap;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class RavenServerSetupPackageProvisionerTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Posts_license_and_domain_and_returns_zip_stream()
    {
        var zip = new byte[] { 0x50, 0x4B, 0x03, 0x04, 9, 8, 7 };
        await using var server = await MockProvisioningServer.StartAsync(zip);

        using var http = new HttpClient { BaseAddress = new Uri(server.BaseAddress) };
        var provisioner = new RavenServerSetupPackageProvisioner(http);

        var input = new LicenseAndDomain(
            new ApplianceLicense { Id = "abc", Name = "test", Keys = ["k1", "k2"] }, "egor-ai");

        await using (var stream = await provisioner.ProvisionAsync(input, CancellationToken.None))
        {
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms);
            Assert.Equal(zip, ms.ToArray());
        }

        // The request must carry the license + domain so the server can build the SetupInfo.
        var sent = JsonNode.Parse(server.LastRequestBody!)!;
        Assert.Equal("egor-ai", (string?)sent["Domain"]);
        Assert.Equal("abc", (string?)sent["License"]!["Id"]);
        Assert.Equal("k1", (string?)sent["License"]!["Keys"]![0]);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Throws_with_status_on_non_success()
    {
        await using var server = await MockProvisioningServer.StartAsync([], statusCode: 500);
        using var http = new HttpClient { BaseAddress = new Uri(server.BaseAddress) };
        var provisioner = new RavenServerSetupPackageProvisioner(http);

        var input = new LicenseAndDomain(new ApplianceLicense { Id = "abc", Name = "n", Keys = ["k"] }, "egor-ai");

        var ex = await Assert.ThrowsAsync<SetupPackageProvisioningException>(
            () => provisioner.ProvisionAsync(input, CancellationToken.None));
        Assert.Equal(HttpStatusCode.InternalServerError, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Throws_when_server_unreachable()
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://nonexistent.invalid") };
        var provisioner = new RavenServerSetupPackageProvisioner(http);

        var input = new LicenseAndDomain(new ApplianceLicense { Id = "abc", Name = "n", Keys = ["k"] }, "egor-ai");

        var ex = await Assert.ThrowsAsync<SetupPackageProvisioningException>(
            () => provisioner.ProvisionAsync(input, CancellationToken.None));
        Assert.Null(ex.StatusCode);
    }
}

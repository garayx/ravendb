using System.Net;
using System.Net.Http;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Bootstrap;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class QuillLicenseDomainResolverTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Resolves_license_and_domain_for_known_token()
    {
        await using var api = await MockQuillApi.StartAsync(
            token: "quill",
            license: new ApplianceLicense { Id = "abc", Name = "test", Keys = ["k1", "k2"] },
            domain: "egor-ai");

        using var http = new HttpClient { BaseAddress = new Uri(api.BaseAddress) };
        var resolver = new QuillLicenseDomainResolver(http);

        var result = await resolver.ResolveAsync("quill", CancellationToken.None);

        Assert.Equal("egor-ai", result.Domain);
        Assert.Equal("abc", result.License.Id);
        Assert.Equal("test", result.License.Name);
        Assert.Equal(new[] { "k1", "k2" }, result.License.Keys);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Throws_with_NotFound_when_token_unknown()
    {
        await using var api = await MockQuillApi.StartAsync(
            token: "quill",
            license: new ApplianceLicense { Id = "abc", Name = "test", Keys = ["k1"] },
            domain: "egor-ai");

        using var http = new HttpClient { BaseAddress = new Uri(api.BaseAddress) };
        var resolver = new QuillLicenseDomainResolver(http);

        var ex = await Assert.ThrowsAsync<LicenseResolutionException>(
            () => resolver.ResolveAsync("not-the-token", CancellationToken.None));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Throws_when_upstream_unreachable()
    {
        // Unresolvable host: the GET throws a transport error which must surface as a
        // LicenseResolutionException (no status code), not escape raw.
        using var http = new HttpClient { BaseAddress = new Uri("http://nonexistent.invalid") };
        var resolver = new QuillLicenseDomainResolver(http);

        var ex = await Assert.ThrowsAsync<LicenseResolutionException>(
            () => resolver.ResolveAsync("quill", CancellationToken.None));
        Assert.Null(ex.StatusCode);
    }
}

using System.Net;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Raven.AiAppliance.Hosting;
using Xunit;

namespace AiApplianceTests;

public class HealthEndpointsTests : IClassFixture<HealthEndpointsTests.Factory>
{
    private readonly Factory _factory;

    public HealthEndpointsTests(Factory factory) => _factory = factory;

    [Fact]
    public async Task Returns_503_before_readiness_flag_is_set()
    {
        _factory.Ready.MarkFailed("not yet");
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task Returns_200_once_readiness_flag_flips()
    {
        _factory.Ready.MarkReady();
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    public sealed class Factory : WebApplicationFactory<Program>
    {
        public IServerReady Ready { get; } = new ServerReadyFlag();

        protected override IHost CreateHost(IHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                // Replace the registered flag with our controllable instance.
                services.RemoveAll<IServerReady>();
                services.AddSingleton<IServerReady>(Ready);

                // Drop only RavenReadinessService — RemoveAll<IHostedService>()
                // would also kill GenericWebHostService and leave the test host
                // unable to serve traffic.
                var toRemove = services
                    .Where(d => d.ImplementationType == typeof(RavenReadinessService))
                    .ToList();
                foreach (var d in toRemove)
                    services.Remove(d);
            });
            return base.CreateHost(builder);
        }
    }
}

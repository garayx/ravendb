using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Infrastructure;

internal sealed class RavenHealthCheck(IServerReady ready) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (ready.IsReady)
            return Task.FromResult(HealthCheckResult.Healthy());

        var description = ready.LastError is { Length: > 0 } err
            ? $"RavenDB not ready: {err}"
            : "RavenDB not ready yet.";
        return Task.FromResult(HealthCheckResult.Unhealthy(description));
    }
}

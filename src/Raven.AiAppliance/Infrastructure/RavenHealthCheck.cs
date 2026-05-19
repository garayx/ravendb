using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.Infrastructure;

internal sealed class RavenHealthCheck(IBootstrapState bootstrap) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (bootstrap.Phase == BootstrapPhase.Ready)
            return Task.FromResult(HealthCheckResult.Healthy());

        var description = bootstrap.Reason is { Length: > 0 } reason
            ? $"appliance not ready ({bootstrap.Phase.ToString().ToLowerInvariant()}): {reason}"
            : $"appliance not ready: {bootstrap.Phase.ToString().ToLowerInvariant()}";
        return Task.FromResult(HealthCheckResult.Unhealthy(description));
    }
}

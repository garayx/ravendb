using System.Net;

namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Thrown when the RavenDB provisioning endpoint fails to produce a setup package.
/// <see cref="StatusCode"/> carries the endpoint's HTTP status when the failure was an
/// unsuccessful response; it is <c>null</c> for transport failures (the local server being
/// unreachable mid-activation).
/// </summary>
public sealed class SetupPackageProvisioningException(HttpStatusCode? statusCode, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public HttpStatusCode? StatusCode { get; } = statusCode;
}

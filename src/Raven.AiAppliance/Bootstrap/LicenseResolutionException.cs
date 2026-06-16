using System.Net;

namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Thrown when the license API cannot produce a usable <see cref="LicenseAndDomain"/>.
/// <see cref="StatusCode"/> carries the upstream HTTP status when the failure was an
/// unsuccessful response (so the redeem endpoint can echo it); it is <c>null</c> for transport
/// failures (DNS/TLS/socket) and malformed/incomplete payloads.
/// </summary>
public sealed class LicenseResolutionException(HttpStatusCode? statusCode, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public HttpStatusCode? StatusCode { get; } = statusCode;
}

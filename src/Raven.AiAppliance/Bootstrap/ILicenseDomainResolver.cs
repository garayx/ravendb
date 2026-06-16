namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Resolves a license token (the operator's "key") into the license + preregistered domain
/// via the upstream license API. Production hits api.ravendb.net's Quill endpoint; the demo
/// uses a mock. Failures surface as <see cref="LicenseResolutionException"/> so the redeem
/// endpoint can map them to the right HTTP problem response.
/// </summary>
public interface ILicenseDomainResolver
{
    Task<LicenseAndDomain> ResolveAsync(string token, CancellationToken ct);
}

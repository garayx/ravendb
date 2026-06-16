namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Turns a resolved <see cref="LicenseAndDomain"/> into a RavenDB setup-package zip. The
/// appliance can't reference Raven.Server (where the setup-wizard lives), so the real
/// implementation drives the RavenDB server's headless provisioning endpoint over loopback;
/// the server runs the Let's Encrypt wizard and returns the package. Failures surface as
/// <see cref="SetupPackageProvisioningException"/>.
/// </summary>
public interface ISetupPackageProvisioner
{
    /// <summary>
    /// Returns the setup-package zip as a readable stream positioned at the start. The caller
    /// owns the returned stream and disposes it.
    /// </summary>
    Task<Stream> ProvisionAsync(LicenseAndDomain licenseAndDomain, CancellationToken ct);
}

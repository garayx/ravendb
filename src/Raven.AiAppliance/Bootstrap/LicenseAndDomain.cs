using Raven.AiAppliance.AiHelper;

namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// The pair returned by the license API for a redeemed token: the license to activate the
/// RavenDB server with, and the preregistered domain name the Let's Encrypt certificate is
/// issued for (e.g. <c>egor-ai</c>). The setup wizard turns this pair into a setup package.
/// </summary>
public sealed record LicenseAndDomain(ApplianceLicense License, string Domain);

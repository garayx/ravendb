using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Commercial.SetupWizard
{
    /// <summary>
    /// Headless entry point for the AI Appliance's first-run provisioning. Turns a
    /// <c>{ license, domain }</c> pair into the inputs the Let's Encrypt setup wizard
    /// (<see cref="SetupManager.SetupLetsEncryptTask"/>) needs, reusing the same domain-claim
    /// the interactive wizard uses to discover the root domain + account e-mail from the license.
    /// </summary>
    public static class ApplianceSetupManager
    {
        internal const string LocalNodeTag = "A";

        /// <summary>
        /// Builds a single-node (tag "A") <see cref="SetupInfo"/> for Let's Encrypt provisioning.
        /// <see cref="SetupInfoBase.ZipOnly"/> is set so the provisioning server only generates the
        /// package — it does not write its own settings.json/cert; the appliance extracts the
        /// package and s6 applies it on the RavenDB restart.
        /// </summary>
        public static SetupInfo BuildSetupInfo(License license, string domain, string rootDomain, string email,
            int httpPort, int tcpPort, List<string> addresses)
        {
            return new SetupInfo
            {
                Domain = domain,
                RootDomain = rootDomain,
                Email = email,
                License = license,
                LocalNodeTag = LocalNodeTag,
                ZipOnly = true,
                RegisterClientCert = false,
                NodeSetupInfos = new Dictionary<string, NodeInfo>
                {
                    [LocalNodeTag] = new NodeInfo
                    {
                        Port = httpPort != 0 ? httpPort : Constants.Network.DefaultSecuredRavenDbHttpPort,
                        TcpPort = tcpPort != 0 ? tcpPort : Constants.Network.DefaultSecuredRavenDbTcpPort,
                        Addresses = addresses is { Count: > 0 } ? addresses : new List<string> { "0.0.0.0" }
                    }
                }
            };
        }

        /// <summary>
        /// Discovers the root domain + account e-mail for a license-owned subdomain by claiming it
        /// at api.ravendb.net (<c>POST /api/v1/dns-n-cert/claim</c>), mirroring the wizard's
        /// claim step. Throws when the claim fails or the response is missing the expected fields.
        /// </summary>
        public static async Task<(string RootDomain, string Email)> ClaimDomainAsync(
            License license, string domain, JsonOperationContext context, CancellationToken token)
        {
            var claimInfo = new ClaimDomainInfo { Domain = domain, License = license };
            using var payload = context.ReadObject(claimInfo.ToJson(), "appliance/claim-domain");

            using var content = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
            using var response = await ApiHttpClient.PostAsync("/api/v1/dns-n-cert/claim", content, token: token).ConfigureAwait(false);
            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
                throw new InvalidOperationException(
                    $"Failed to claim domain '{domain}' from {ApiHttpClient.ApiRavenDbNet}: {(int)response.StatusCode} {responseString}");

            using var resultStream = new MemoryStream(Encoding.UTF8.GetBytes(responseString));
            using var result = await context.ReadForMemoryAsync(resultStream, "appliance/claim-result", token).ConfigureAwait(false);

            if (result.TryGet("RootDomains", out BlittableJsonReaderArray rootDomains) == false || rootDomains.Length == 0)
                throw new InvalidOperationException(
                    $"Claim response for '{domain}' did not include any RootDomains. Response: {responseString}");

            var rootDomain = rootDomains[0].ToString();

            if (result.TryGet("Email", out string email) == false || string.IsNullOrEmpty(email))
            {
                if (result.TryGet("Emails", out BlittableJsonReaderArray emails) && emails.Length > 0)
                    email = emails[0].ToString();
            }

            if (string.IsNullOrEmpty(email))
                throw new InvalidOperationException(
                    $"Claim response for '{domain}' did not include an account e-mail. Response: {responseString}");

            return (rootDomain, email);
        }
    }
}

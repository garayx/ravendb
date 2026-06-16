using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.AiHelper;

namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Drives RavenDB's headless provisioning endpoint (<c>POST /setup/appliance/provision</c>)
/// over loopback: posts <c>{ License, Domain }</c>, the server runs the Let's Encrypt setup
/// wizard and returns the setup-package zip. Registered as a typed <see cref="HttpClient"/>
/// with <c>BaseAddress</c> set to <c>ApplianceOptions.RavenUrl</c>.
/// </summary>
public sealed class RavenServerSetupPackageProvisioner(HttpClient httpClient) : ISetupPackageProvisioner
{
    public const string ProvisionPath = "setup/appliance/provision";

    // PascalCase (no naming policy) so the wire shape matches RavenDB's JSON deserialization
    // on the server endpoint — not PostAsJsonAsync's camelCase Web default.
    private static readonly JsonSerializerOptions JsonOptions = new();

    public async Task<Stream> ProvisionAsync(LicenseAndDomain licenseAndDomain, CancellationToken ct)
    {
        var payload = new SetupPackageProvisionRequest
        {
            License = licenseAndDomain.License,
            Domain = licenseAndDomain.Domain,
        };

        HttpResponseMessage response;
        try
        {
            response = await httpClient.PostAsJsonAsync(ProvisionPath, payload, JsonOptions, ct);
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            throw new SetupPackageProvisioningException(
                statusCode: null, $"RavenDB provisioning endpoint unreachable: {e.Message}", e);
        }

        using (response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                throw new SetupPackageProvisioningException(
                    response.StatusCode,
                    $"RavenDB provisioning endpoint returned {(int)response.StatusCode} {response.ReasonPhrase}: {body}");
            }

            // The package is produced by the local, trusted RavenDB server and is small
            // (<100 KB), so buffering it is cheap and lets us drop the HTTP response here
            // rather than tying its lifetime to the returned stream.
            var bytes = await response.Content.ReadAsByteArrayAsync(ct);
            return new MemoryStream(bytes, writable: false);
        }
    }

    private sealed class SetupPackageProvisionRequest
    {
        public ApplianceLicense License { get; set; } = null!;
        public string Domain { get; set; } = null!;
    }
}

using System.Net;
using System.Text.Json;
using Raven.AiAppliance.AiHelper;

namespace Raven.AiAppliance.Bootstrap;

/// <summary>
/// Resolves a license token against api.ravendb.net's Quill endpoint
/// (<c>GET /api/v1/quill/licenses/{token}</c>, PR #3003), which returns
/// <c>{ license, domain }</c>. Registered as a typed <see cref="HttpClient"/> with
/// <c>BaseAddress</c> set to <c>ApplianceOptions.LicenseApiUrl</c>.
/// </summary>
public sealed class QuillLicenseDomainResolver(HttpClient httpClient) : ILicenseDomainResolver
{
    private const string LicensesPath = "api/v1/quill/licenses/";

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public async Task<LicenseAndDomain> ResolveAsync(string token, CancellationToken ct)
    {
        HttpResponseMessage response;
        try
        {
            response = await httpClient.GetAsync(LicensesPath + Uri.EscapeDataString(token), ct);
        }
        catch (Exception e) when (e is HttpRequestException ||
                                  (e is OperationCanceledException && ct.IsCancellationRequested == false))
        {
            // DNS/TLS/socket failure or an HttpClient timeout (not caller cancellation): no status
            // code to echo. Caller cancellation propagates.
            throw new LicenseResolutionException(statusCode: null, $"license API unreachable: {e.Message}", e);
        }

        using (response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                throw new LicenseResolutionException(
                    response.StatusCode,
                    $"license API returned {(int)response.StatusCode} {response.ReasonPhrase}");
            }

            var json = await response.Content.ReadAsStringAsync(ct);

            QuillLicenseResponse? parsed;
            try
            {
                parsed = JsonSerializer.Deserialize<QuillLicenseResponse>(json, JsonOptions);
            }
            catch (JsonException e)
            {
                throw new LicenseResolutionException(statusCode: null, "license API returned an unparseable response.", e);
            }

            if (parsed?.License is null || string.IsNullOrEmpty(parsed.License.Id))
                throw new LicenseResolutionException(statusCode: null, "license API response is missing the license.");

            if (string.IsNullOrWhiteSpace(parsed.Domain))
                throw new LicenseResolutionException(statusCode: null, "license API response is missing the domain.");

            return new LicenseAndDomain(parsed.License, parsed.Domain);
        }
    }

    private sealed class QuillLicenseResponse
    {
        public ApplianceLicense? License { get; set; }
        public string? Domain { get; set; }
    }
}

# Handoff — AI Appliance Bootstrap: license-token → Let's Encrypt provisioning

> Working notes for picking up this branch. **Not meant for the upstream PR** — delete before merging to v7.2.

- **Branch:** `RavenDB-26629_150626`
- **Code commit:** `b869ad16fb4` — "RavenDB-26629 AI Appliance: provision Let's Encrypt cert from license token" (22 files, +1079/-48)
- **Status:** implemented, builds clean, unit tests green, gated LE e2e verified live against Pebble. Not pushed; no PR yet.

---

## Why

Previously the AI Appliance activated by downloading a **pre-built setup-package zip** (server cert, settings, license, admin cert) and extracting it. We reworked first-run activation to match the api.ravendb.net **"Quill"** flow (internals PR #3003 / RavenDB-26783):

1. Operator submits a **license token** (`/api/bootstrap/redeem-license`).
2. Appliance calls the license API → gets back **`{ license, domain }`**.
3. Appliance hands `{license, domain}` to RavenDB, which **reuses the setup wizard** to mint a **Let's Encrypt** cert, register the license, write settings, and produce the setup package.
4. Appliance extracts the package and (via s6) restarts RavenDB **secured**.

For now the license API is **mocked**; the hardcoded demo domain is `egor-ai` (root `ravendb.run`, node `a.egor-ai.ravendb.run`).

---

## Architecture

```
POST /api/bootstrap/redeem-license {licenseKey}        [Raven.AiAppliance]
  → ILicenseDomainResolver        GET {LicenseApiUrl}/api/v1/quill/licenses/{token}  → {license, domain}
  → ISetupPackageProvisioner      POST {RavenUrl}/setup/appliance/provision {License, Domain}  → zip
  → (existing) stream→temp + 32MB cap + zip-slip-safe extract → /setup/ + admin-thumbprint
  → (existing) s6 restart RavenDB | inline MarkReady (no s6)

POST /setup/appliance/provision                         [Raven.Server]
  → ApplianceSetupManager.ClaimDomainAsync   (api.ravendb.net /dns-n-cert/claim → RootDomain + Email)
  → ApplianceSetupManager.BuildSetupInfo     (single node "A", ZipOnly=true)
  → SetupManager.SetupLetsEncryptTask        (ACME via Core.AcmeUrl, DNS-01 challenge) → zip
```

**Key constraint that shaped this:** `Raven.AiAppliance` references only `Raven.Client` + `Sparrow` (not `Raven.Server`), so the wizard/cert work runs **in the RavenDB process** behind the new endpoint and the appliance drives it over loopback. The **setup-package zip stays the internal hand-off format** — only its *source* changed (wizard-generated vs. pre-built).

---

## What was implemented

**Appliance — `src/Raven.AiAppliance/Bootstrap/` (new):**
- `LicenseAndDomain`, `ILicenseDomainResolver`, `QuillLicenseDomainResolver`, `LicenseResolutionException`
- `ISetupPackageProvisioner`, `RavenServerSetupPackageProvisioner`, `SetupPackageProvisioningException`
- `Endpoints/BootstrapEndpoints.cs` (mod) — `RedeemLicenseAsync` = resolve → provision → existing extract/restart; typed failure→HTTP mapping; demo `SetupPackageZipPath` short-circuit kept.
- `Program.cs` (mod) — DI: both as typed `HttpClient`s (`LicenseApiUrl` / `RavenUrl`).

**Server — `src/Raven.Server/`:**
- `Web/System/ApplianceSetupHandler.cs` (new) — `POST /setup/appliance/provision`, `UnauthenticatedClients` (loopback trust), refused once secured, **not** gated on `Setup.Mode=Initial`.
- `Commercial/SetupWizard/ApplianceSetupManager.cs` (new) — `ClaimDomainAsync` + `BuildSetupInfo`.
- `Commercial/ApplianceProvisionSetupInfo.cs` (new) — request DTO; registered in `Json/JsonDeserializationServer.cs` (mod).

**Tests — `test/`:**
- Unit (hermetic, `AiApplianceTests`): `QuillLicenseDomainResolverTests`, `RavenServerSetupPackageProvisionerTests`, `BootstrapRedeemEndpointTests`, `ApplianceSetupManagerTests`. Fixtures `MockQuillApi`, `MockProvisioningServer`. `ApplianceWebApplicationFactory` gained a `configureServices` hook.
- Gated e2e (`SlowTests`): `AuthenticationLetsEncryptTests.Appliance.cs` (partial class, reuses `SetupClusterInfo`/`GetCertificateFromLetsEncrypt` plumbing) — `ApplianceProvisionEndpoint_GetsPebbleCertificate_AndBootsSecuredNode`.
- `ApplianceFullFlowTests` (mod) — switched to the demo zip short-circuit (stays green/gated; remains the CDC→agent→iFrame demo).

---

## Verification (done this session)

- `dotnet build RavenDB.sln -c Release` → **0 errors**.
- 9 hermetic unit tests → **green**: `dotnet test test/AiApplianceTests --filter "FullyQualifiedName~QuillLicenseDomainResolverTests|FullyQualifiedName~RavenServerSetupPackageProvisionerTests|FullyQualifiedName~BootstrapRedeemEndpointTests|FullyQualifiedName~ApplianceSetupManagerTests"`
- Gated LE + secured-boot e2e → **ran GREEN live against Pebble** (claim → ACME cert → secured node boot → TLS doc store).

### Running the gated e2e (the fiddly part)

```bash
# 1. Pebble ACME server (the user's CI exposes pebble:14000; locally use a container):
docker run -d --name pebble -p 14000:14000 -p 15000:15000 -e PEBBLE_VA_NOSLEEP=1 \
  ghcr.io/letsencrypt/pebble:latest -config /test/config/pebble-config.json -dnsserver 8.8.8.8:53

# 2. Trust Pebble's signing CA so RavenDB's LetsEncryptClient (RavenHttpClient) accepts the ACME TLS:
docker cp pebble:/test/certs/pebble.minica.pem .
certutil -user -addstore -f Root pebble.minica.pem          # CurrentUser store, no admin

# 3. Run (RAVEN_LICENSE must be set in the env):
RAVEN_SKIP_INTEGRATION_TESTS=false RAVEN_IS_RUNNING_ON_CI=true RAVEN_PEBBLE_URL=https://localhost:14000/dir \
  dotnet test test/SlowTests --filter "FullyQualifiedName~ApplianceProvisionEndpoint_GetsPebbleCertificate_AndBootsSecuredNode"

# cleanup: certutil -user -delstore Root "minica root ca <id>" ; docker rm -f pebble
```

**Gotchas learned:**
- `[RavenIntegrationRetryFact]` self-skips unless `RAVEN_IS_RUNNING_ON_CI=true` **and** `RAVEN_SKIP_INTEGRATION_TESTS=false` (the dev profile sets the latter to `True`).
- The shared domain name `RavenClusterTest<machine>` gets **stuck-owned** at api.ravendb.net ("already owned by someone else") and breaks RavenDB's own `CanGetPebbleCertificate` too — so the appliance test claims a **unique** subdomain each run.
- Pebble's ACME endpoint is self-signed; RavenDB's `LetsEncryptClient` does not bypass cert validation, so its CA must be trusted (step 2).
- Each gated run leaves a unique registered subdomain at api.ravendb.net (harmless test artifact).

---

## What's left / open

- **Push + PR** — not done. Commit format `RavenDB-####`; use `.github/pull_request_template.md`.
- **Optional full-chain test:** add `ApplianceOptions.ProvisioningHttpPort/TcpPort` (default 443/38888) + provisioner forwarding so `/api/bootstrap/redeem-license` can drive the real RavenDB endpoint end-to-end (today the e2e calls the endpoint directly; the appliance→endpoint hop is unit-tested).
- **Contract reconciliation:** confirm PR #3003's finalized GET-by-token field names; `ILicenseDomainResolver` parses `{license, domain}` case-insensitively (one-file change).
- **Container/prod LE:** image needs `Core.AcmeUrl` + network egress to api.ravendb.net + Let's Encrypt; `docker/ai-appliance/ravendb-settings.json` stays `Setup.Mode=Unsecured` (correct); s6 restart path unchanged — **not** smoke-tested in a real container build.
- **Pre-existing, unrelated failure:** `AiConnectionStringsEndpointsTests.GetByName_redacts_openai_api_key` (separate subsystem; left alone).
- `MockLicenseApi.cs` is now orphaned (the old zip-serving mock) — safe to delete in cleanup.

---

## Design decisions (rationale)

- **Provisioning runs server-side, appliance drives it** — forced by the assembly boundary; also matches how RavenDB's real setup wizard works.
- **`ZipOnly=true`** in `BuildSetupInfo` — the provisioning server must NOT write its own `settings.json`/cert; the appliance + s6 apply the package. (`SettingsZipFileHelper` only does local writes when `ZipOnly==false`.)
- **Endpoint claims RootDomain+Email internally** (same mechanism as `AuthenticationLetsEncryptTests.SetupClusterInfo`) so the appliance contract stays `{license, domain}`. The endpoint also accepts them explicitly to skip the claim.
- **Demo `SetupPackageZipPath` short-circuit kept** — the existing Docker demo + `ApplianceFullFlowTests` still work via the pre-built zip.

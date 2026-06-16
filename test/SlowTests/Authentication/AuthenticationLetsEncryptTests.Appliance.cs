using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public partial class AuthenticationLetsEncryptTests
    {
        /// <summary>
        /// End-to-end for the AI Appliance's headless provisioning endpoint
        /// (<c>POST /setup/appliance/provision</c>): given a license + preregistered domain the
        /// server runs the Let's Encrypt setup wizard against Pebble and returns the setup package,
        /// then we boot a secured node from that package and prove a TLS client request succeeds.
        /// Mirrors <see cref="CanGetPebbleCertificate"/> + the renew tests' secured-boot pattern,
        /// but drives the appliance endpoint instead of <c>/setup/letsencrypt</c>. Gated on a
        /// running Pebble ACME server via RAVEN_PEBBLE_URL.
        /// </summary>
        [RavenIntegrationRetryFact(delayBetweenRetriesMs: 1000)]
        public async Task ApplianceProvisionEndpoint_GetsPebbleCertificate_AndBootsSecuredNode()
        {
            var acmeUrl = Environment.GetEnvironmentVariable("RAVEN_PEBBLE_URL") ?? string.Empty;
            Assert.NotEmpty(acmeUrl);

            RemoveAcmeCache(acmeUrl);

            SetupLocalServer();
            Server.Configuration.Core.AcmeUrl = acmeUrl;
            Server.ServerStore.Configuration.Core.SetupMode = SetupMode.Initial;
            await Server.ServerStore.EnsureNotPassiveAsync();
            var license = Server.ServerStore.LoadLicense();

            // A fresh, unique subdomain each run. The other Pebble tests reuse a shared
            // "RavenClusterTest<machine>" name that can get stuck registered at api.ravendb.net
            // ("already owned by someone else"); a unique name sidesteps that and also exercises
            // the endpoint's own internal domain claim (we deliberately don't pass RootDomain/Email).
            var domain = "raventest" + Guid.NewGuid().ToString("N").Substring(0, 12);

            var (port, socketPort) = ReservePort();
            var (tcpPort, socketTcpPort) = ReservePort();
            var sockets = new List<Socket> { socketPort, socketTcpPort };
            Server.ForTestingPurposesOnly().ReservedSockets = sockets;

            var serverCert = await ProvisionSecuredViaApplianceEndpoint(license, domain, port, tcpPort, acmeUrl);
            Server.Dispose();

            // Boot the secured node on the reserved sockets using the settings the endpoint wrote
            // into the package (queued via DoNotReuseServer in the helper above).
            UseNewLocalServer(sockets: sockets);

            var clientCert = SecretProtection.HasCertificateClientAuthEnhancedKeyUsage(serverCert)
                ? serverCert
                : CertificateUtils.CreateClientCertificateFromServerCertificate(serverCert, out _);

            using (var store = GetDocumentStore(new Options { AdminCertificate = clientCert, ClientCertificate = clientCert }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Provisioned = true }, "markers/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                    Assert.NotNull(session.Load<object>("markers/1"));
            }
        }

        /// <summary>
        /// Drives <c>POST /setup/appliance/provision</c> with just the license + domain + ports
        /// (the endpoint claims the root domain / e-mail itself), extracts the returned package,
        /// and queues the secured settings for the next server via <c>DoNotReuseServer</c>.
        /// Mirrors <see cref="GetCertificateFromLetsEncrypt"/> but for the appliance endpoint.
        /// </summary>
        private async Task<X509Certificate2> ProvisionSecuredViaApplianceEndpoint(
            License license, string domain, int httpPort, int tcpPort, string acmeUrl)
        {
            X509Certificate2 serverCert;
            using (var store = GetDocumentStoreForServerOnly())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var payload = new DynamicJsonValue
                {
                    ["License"] = license.ToJson(),
                    ["Domain"] = domain,
                    ["HttpPort"] = httpPort,
                    ["TcpPort"] = tcpPort,
                    ["Addresses"] = new DynamicJsonArray(new[] { "127.0.0.1" })
                };

                var command = new ProvisionApplianceSetupCommand(store.Conventions, context, payload);
                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result is { Length: > 0 }, "provision endpoint returned an empty package");

                var zipBytes = command.Result;

                BlittableJsonReaderObject settingsJsonObject;
                byte[] serverCertBytes;
                try
                {
                    settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(
                        zipBytes, "A", context, out serverCertBytes, out serverCert, out _, out _, out _, out _);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Unable to extract setup information from the provisioned package.", e);
                }

                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.TcpServerUrls), out string tcpServerUrl);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

                var tempFileName = GetTempFileName();
                await File.WriteAllBytesAsync(tempFileName, serverCertBytes);

                IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                    [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                    [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = publicServerUrl,
                    [RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = tcpServerUrl,
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                    [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                    [RavenConfiguration.GetKey(x => x.Core.AcmeUrl)] = acmeUrl
                };

                DoNotReuseServer(customSettings);
            }

            return serverCert;
        }

        private sealed class ProvisionApplianceSetupCommand : RavenCommand<byte[]>
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _payload;

            public ProvisionApplianceSetupCommand(DocumentConventions conventions, JsonOperationContext context, DynamicJsonValue payload)
            {
                _conventions = conventions;
                _payload = context.ReadObject(payload, "appliance-provision");
                ResponseType = RavenCommandResponseType.Raw;
                Timeout = TimeSpan.FromMinutes(10);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/setup/appliance/provision";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _payload).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                if (response == null)
                    return;

                var ms = new MemoryStream();
                stream.CopyTo(ms);
                Result = ms.ToArray();
            }
        }
    }
}

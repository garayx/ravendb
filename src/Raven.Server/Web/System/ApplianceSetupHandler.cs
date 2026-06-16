using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class ApplianceSetupHandler : ServerRequestHandler
    {
        /// <summary>
        /// Headless setup-wizard entry point for the AI Appliance. Given a license + preregistered
        /// domain it runs the Let's Encrypt setup wizard and returns the setup-package zip for the
        /// appliance to extract + apply on the RavenDB restart.
        /// </summary>
        /// <remarks>
        /// Unlike the interactive <c>/setup/*</c> endpoints this does NOT require
        /// <c>Setup.Mode=Initial</c> — the appliance keeps RavenDB Unsecured (loopback-only) until
        /// activation, so provisioning must run outside Initial mode. It is refused once the server
        /// is already secured, and <see cref="SetupManager.SetupLetsEncryptTask"/> additionally
        /// asserts the node is not yet part of a cluster.
        /// </remarks>
        [RavenAction("/setup/appliance/provision", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ProvisionApplianceSetupPackage()
        {
            if (ServerStore.Server.Certificate?.ServerCertificate != null)
                throw new InvalidOperationException(
                    "The server is already secured; /setup/appliance/provision is only available before activation.");

            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var token = operationCancelToken.Token;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "appliance-provision"))
            {
                var request = JsonDeserializationServer.ApplianceProvisionSetupInfo(json);

                if (request.License?.Keys is null || request.License.Keys.Count == 0)
                    throw new BadRequestException("A license with keys must be provided.");

                if (string.IsNullOrWhiteSpace(request.Domain))
                    throw new BadRequestException("A domain must be provided.");

                var rootDomain = request.RootDomain;
                var email = request.Email;
                if (string.IsNullOrEmpty(rootDomain) || string.IsNullOrEmpty(email))
                {
                    var (claimedRootDomain, claimedEmail) =
                        await ApplianceSetupManager.ClaimDomainAsync(request.License, request.Domain, context, token);

                    if (string.IsNullOrEmpty(rootDomain))
                        rootDomain = claimedRootDomain;
                    if (string.IsNullOrEmpty(email))
                        email = claimedEmail;
                }

                var setupInfo = ApplianceSetupManager.BuildSetupInfo(
                    request.License, request.Domain, rootDomain, email,
                    request.HttpPort, request.TcpPort, request.Addresses);

                var operationId = ServerStore.Operations.GetNextOperationId();
                var operationResult = await ServerStore.Operations.AddLocalOperation(
                    operationId,
                    OperationType.Setup,
                    "Provisioning the AI Appliance setup package with a Let's Encrypt certificate.",
                    detailedDescription: null,
                    progress => SetupManager.SetupLetsEncryptTask(progress, setupInfo, ServerStore, token),
                    persistProgressOnFaultedStatus: true,
                    token: operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                HttpContext.Response.ContentType = "application/zip";
                await HttpContext.Response.Body.WriteAsync(zip, 0, zip.Length);
            }
        }
    }
}

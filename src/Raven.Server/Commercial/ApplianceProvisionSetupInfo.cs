using System.Collections.Generic;

namespace Raven.Server.Commercial
{
    /// <summary>
    /// Request body for <c>POST /setup/appliance/provision</c>. The AI Appliance posts the
    /// license + preregistered domain it got from the license API; the server runs the Let's
    /// Encrypt setup wizard and returns the setup-package zip. <see cref="RootDomain"/> and
    /// <see cref="Email"/> are optional — when omitted the server discovers them from the
    /// license via the domain claim (same as the interactive wizard). Ports/addresses are
    /// optional too (defaulted for the container; tests pass reserved ports).
    /// </summary>
    public sealed class ApplianceProvisionSetupInfo
    {
        public License License { get; set; }
        public string Domain { get; set; }
        public string Email { get; set; }
        public string RootDomain { get; set; }
        public int HttpPort { get; set; }
        public int TcpPort { get; set; }
        public List<string> Addresses { get; set; }
    }
}

using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLicenseType : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ServerLicenseType(ServerStore store)
            : base("1.9.1")
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var status = _store.LicenseManager.GetLicenseStatus();
            return new OctetString(status.Type.ToString());
        }
    }
}

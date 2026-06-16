using System;
using System.Collections.Generic;
using FastTests;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class ApplianceSetupManagerTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance | RavenTestCategory.Certificates)]
    public void BuildSetupInfo_produces_single_node_A_zip_only_setup()
    {
        var license = new License { Id = Guid.NewGuid(), Name = "test", Keys = ["k1", "k2"] };

        var setupInfo = ApplianceSetupManager.BuildSetupInfo(
            license,
            domain: "egor-ai",
            rootDomain: "ravendb.run",
            email: "ops@egor-ai.example",
            httpPort: 1443,
            tcpPort: 38888,
            addresses: ["0.0.0.0"]);

        Assert.Equal("egor-ai", setupInfo.Domain);
        Assert.Equal("ravendb.run", setupInfo.RootDomain);
        Assert.Equal("ops@egor-ai.example", setupInfo.Email);
        Assert.Same(license, setupInfo.License);
        Assert.Equal("A", setupInfo.LocalNodeTag);

        // Zip-only: the provisioning server hands the package back for the appliance to extract
        // and s6 to apply on restart — it must not write its own settings.json / cert.
        Assert.True(setupInfo.ZipOnly);

        var node = Assert.Contains("A", setupInfo.NodeSetupInfos);
        Assert.Equal(1443, node.Port);
        Assert.Equal(38888, node.TcpPort);
        Assert.Equal(new[] { "0.0.0.0" }, node.Addresses);
    }

    [RavenFact(RavenTestCategory.AiAppliance | RavenTestCategory.Certificates)]
    public void BuildSetupInfo_defaults_addresses_to_bind_all_when_omitted()
    {
        var license = new License { Id = Guid.NewGuid(), Name = "test", Keys = ["k1"] };

        var setupInfo = ApplianceSetupManager.BuildSetupInfo(
            license, domain: "egor-ai", rootDomain: "ravendb.run", email: "ops@egor-ai.example",
            httpPort: 0, tcpPort: 0, addresses: null);

        var node = Assert.Contains("A", setupInfo.NodeSetupInfos);
        Assert.Equal(new[] { "0.0.0.0" }, node.Addresses);
    }
}

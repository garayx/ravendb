﻿using Tests.Infrastructure;
using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Xunit.Abstractions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_16895 : RavenTestBase
    {
        public RavenDB_16895(ITestOutputHelper output) : base(output)
        {
        }

        [LicenseRequiredFact]
        public async Task Should_Wait_For_License_To_Be_Reloaded_After_Activation()
        {
            DoNotReuseServer();

            Assert.Contains("AGPL", Server.ServerStore.LicenseManager.LicenseStatus.Status);

            Server.ServerStore.ForTestingPurposesOnly().BeforePutLicenseCommandHandledInOnValueChanged += () => Thread.Sleep(TimeSpan.FromSeconds(2));

            await Server.ServerStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
            await Server.ServerStore.LicenseManager.TryActivateLicenseAsync(throwOnActivationFailure: true);

            Assert.Contains("Commercial", Server.ServerStore.LicenseManager.LicenseStatus.Status);
        }
    }
}

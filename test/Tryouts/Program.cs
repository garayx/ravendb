using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using FastTests.Voron.Sets;
using FastTests.Corax.Bugs;
using FastTests.Sharding;
using RachisTests.DatabaseCluster;
using Raven.Server.Utils;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server.Documents.PeriodicBackup;
using SlowTests.Sharding.Cluster;
using Raven.Server.Documents.Subscriptions;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);

        for (int i = 0; i < 1000; i++)
        {
            Console.WriteLine($"Starting to run {i}");

            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new SubscriptionsWithReshardingTests(testOutputHelper))
                {
                    var p = System.AppDomain.CurrentDomain.BaseDirectory;
                    var dbPath = Path.Combine(p, "Databases");
                    if (Directory.Exists(dbPath))
                    {
                        Directory.Delete(dbPath, true);
                        Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
                    }


                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    await test.ContinueSubscriptionAfterReshardingInAClusterWithFailover();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
                return;
            }
        }
    }
}

using System;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using Tests.Infrastructure;
using FastTests.Voron.Sets;
using FastTests.Corax.Bugs;
using RachisTests.DatabaseCluster;
using Raven.Server.Utils;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server.Documents.PeriodicBackup;
using SlowTests.Sharding.Cluster;
using Raven.Server.ServerWide.Commands.Subscriptions;

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
                /*using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new ReshardingTests(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    if (i == 1)
                    {

                    }
                    await test.CanMoveOneBucketManually();
                }*/



                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new SubscriptionsWithReshardingTests(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    if (i == 1)
                    {

                    }

                    RecordBatchSubscriptionDocumentsCommand.DocId = null;
                    await test.GetDocumentOnce2();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}

using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using RachisTests;
using SlowTests.Client.Attachments;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);

            TryRemoveDatabasesFolder();
            for (int i = 0; i < 10_000; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new PullReplicationTests(testOutputHelper))
                    {
                        await test.PullReplicationWithIdleShouldWork2();
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


        private static void TryRemoveDatabasesFolder()
        {
            var p = System.AppDomain.CurrentDomain.BaseDirectory;
            var dbPath = Path.Combine(p, "Databases");
            if (Directory.Exists(dbPath))
            {
                try
                {
                    Directory.Delete(dbPath, true);
                    Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
                }
                catch
                {
                    Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
                }
            }
        }
    }
}

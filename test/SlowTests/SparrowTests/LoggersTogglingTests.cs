using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Server;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Utils.Metrics;
using SlowTests.Issues;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests
{
    public class LoggersTogglingTests : RavenTestBase
    {
        public LoggersTogglingTests(ITestOutputHelper output) : base(output)
        {
        }

        //TODO: instance loggers count test
        //TODO: Dispose db, dispose index, dispose server tests

        //TODO: can I create a lock?
        [Theory]
        [InlineData(LogType.Cluster)]
        [InlineData(LogType.Database)]
        [InlineData(LogType.Index)]
        [InlineData(LogType.Instance)]
        [InlineData(LogType.Server)]
        public async Task ShouldWork(LogType type)
        {
            using (RavenServer server = GetNewServer())
            {
                using (var store = GetDocumentStore(new Options { Server = server }))
                {
                    new SampleIndex().Execute(store);
                    var client = store.GetRequestExecutor().HttpClient;
                    var response = await client.GetAsync(store.Urls.First() + "/admin/loggers");
                    string result = response.Content.ReadAsStringAsync().Result;
                    var loggers = JsonConvert.DeserializeObject<LoggerSourceHolders>(result);
                    Assert.NotNull(loggers);

                    CheckLoggers(loggers.Loggers, type, LogMode.None, LogMode.None);

                    var setLoggers = new LoggerHolders { Loggers = new List<LoggerHolder> { new LoggerHolder { Type = type, Mode = LogMode.Information } } };
                    var data = new StringContent(JsonConvert.SerializeObject(setLoggers), Encoding.UTF8, "application/json");
                    var setResponse = await client.PostAsync(store.Urls.First() + "/admin/loggers/set?mode=type", data);
                    Assert.True(setResponse.IsSuccessStatusCode);
                    response = await client.GetAsync(store.Urls.First() + "/admin/loggers");
                    result = response.Content.ReadAsStringAsync().Result;
                    loggers = JsonConvert.DeserializeObject<LoggerSourceHolders>(result);
                    Assert.NotNull(loggers);

                    CheckLoggers(loggers.Loggers, type, LogMode.None, LogMode.Information);
                }
            }
        }

        private static List<string> StaticLoggers = new List<string>()
        {
            nameof(GlobalFlushingBehavior), nameof(MetricsScheduler), nameof(GlobalPrefetchingBehavior), nameof(DiskSpaceChecker)
        };

        private static void CheckLoggers(IEnumerable<LoggingSourceHolderTest> loggers, LogType type, LogMode oldMode, LogMode newMode)
        {
            foreach (var holder in loggers)
            {
                if (holder.Type == type)
                {
                    if (newMode != holder.Mode)
                    {

                        Console.WriteLine($"{holder.Name}, {holder.Type}, {holder.Mode}, {holder.Source}");
                        if (StaticLoggers.Contains(holder.Name))
                            continue;
                    }
                }
                else
                {
                    if (oldMode != holder.Mode)
                    {
                        Console.WriteLine($"{holder.Name}, {holder.Type}, {holder.Mode}, {holder.Source}");

                    }
                }

                Assert.Equal(holder.Type == type ? newMode : oldMode, holder.Mode);

                if (holder.Loggers == null)
                    continue;

                CheckLoggers(holder.Loggers, type, oldMode, newMode);
            }
        }


        //private static void CheckLoggers(BlittableJsonReaderArray loggers, LogType type, LogMode mode)
        //{
        //    foreach (BlittableJsonReaderObject holder in loggers)
        //    {
        //        var getName = holder.TryGetMember("Name", out var bjro) && bjro is string name;
        //        Assert.True(getName);
        //        Console.WriteLine(name);


        //        if (holder.Type == type)
        //            Assert.Equal(mode, holder.Mode);

        //        if (holder.Logger.HasLoggers() == false)
        //            continue;

        //        CheckLoggers(holder.Logger.Loggers, type, mode);
        //    }
        //}

        internal class LoggerHolders
        {
            public List<LoggerHolder> Loggers;
        }

        internal class LoggerSourceHolders
        {
            public List<LoggingSourceHolderTest> Loggers;
        }

        //private class LoggingSourceHolderTest : LoggingSourceHolder
        //{
        //    List<LoggingSourceHolderTest> Loggers;
        //}

        internal class LoggingSourceHolderTest
        {
            public string Name { get; set; }
            public string Source { get; set; }
            public LogType Type { get; set; }
            public LogMode Mode { get; set; }
            public List<LoggingSourceHolderTest> Loggers { get; set; }
        }

        //public override void Dispose()
        //{
        //    base.Dispose();
        //    LoggingSource.Instance.GetLogger<TestBase>(nameof(TestBase)).Dispose();
        //}
    }
}

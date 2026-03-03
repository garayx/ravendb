using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.CDC;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.CDC
{
    [Trait("Category", "CdcSink")]
    public abstract class CdcSinkTestBase : SqlAwareTestBase
    {
        protected CdcSinkTestBase(ITestOutputHelper output) : base(output)
        {
            QueueSuffix = Guid.NewGuid().ToString("N");
        }

        protected string QueueSuffix { get; }

        protected string UsersQueueName => $"users{QueueSuffix}";

        protected List<string> DefaultQueues => new() { UsersQueueName };

        protected AddCdcSinkOperationResult AddCdcSink<T>(DocumentStore src, CdcSinkConfiguration configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddCdcSinkOperation<T>(configuration));
            return addResult;
        }

        private async Task<string[]> GetCdcSinkErrorNotifications(DocumentStore src)
        {
            var databaseInstanceFor = await Databases.GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                var notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("CdcSink_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
                return notifications;
            }
        }
        
        public async Task<CdcSinkErrorInfo> TryErrorFromAlertAsync(string databaseName, CdcSinkConfiguration config)
        {
            //var database = await GetDatabase(databaseName);

            //string tag = config.BrokerType == QueueBrokerType.Kafka ? CdcSinkProcess.KafkaTag : CdcSinkProcess.RabbitMqTag;

            //var errorAlert = database.NotificationCenter.CdcSinkNotifications.GetAlert<CdcSinkErrorsDetails>(tag, $"{config.Name}/{config.Scripts.First().Name}", AlertReason.CdcSink_Error);
            //var consumeErrorAlert = database.NotificationCenter.CdcSinkNotifications.GetAlert<CdcSinkErrorsDetails>(tag, $"{config.Name}/{config.Scripts.First().Name}", AlertReason.CdcSink_ConsumeError);
            //var scriptErrorAlert = database.NotificationCenter.CdcSinkNotifications.GetAlert<CdcSinkErrorsDetails>(tag, $"{config.Name}/{config.Scripts.First().Name}", AlertReason.CdcSink_ScriptError);

            //if (errorAlert.Errors.Count != 0)
            //{
            //    return errorAlert.Errors.First();
            //}
            //if (consumeErrorAlert.Errors.Count != 0)
            //{
            //    return consumeErrorAlert.Errors.First();
            //}
            //if (scriptErrorAlert.Errors.Count != 0)
            //{
            //    return scriptErrorAlert.Errors.First();
            //}

            return null;
        }
        
        protected AsyncManualResetEvent WaitForCdcSinkBatch(DocumentStore store,
            Func<string, CdcSinkProcessStatistics, bool> predicate)
        {
            var database = AsyncHelpers.RunSync(() => GetDatabase(store.Database));

            var amre = new AsyncManualResetEvent();

            database.CdcSinkLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.ScriptName}", x.Statistics))
                    amre.Set();
            };

            return amre;
        }

        protected async Task AssertCdcSinkDoneAsync(AsyncManualResetEvent etlDone, TimeSpan timeout, string databaseName, CdcSinkConfiguration config)
        {
            if (await etlDone.WaitAsync(timeout) == false)
            {
                var error = AsyncHelpers.RunSync(() => TryErrorFromAlertAsync(databaseName, config));

                Assert.Fail($"Queue Sink wasn't done. Error: {error?.Error}");
            }
        }

        protected class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }

            public string FullName { get; set; }
        }
    }
}

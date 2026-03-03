using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CDC
{
    public class UpdateCdcSinkCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public long TaskId { get; protected set; }

        public CdcSinkConfiguration Configuration { get; protected set; }
        
        public UpdateCdcSinkCommand()
        {
            // for deserialization
        }

        public UpdateCdcSinkCommand(long taskId, CdcSinkConfiguration configuration, string databaseName,
            string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.CdcSink, DatabaseName, null).UpdateDatabaseRecord(
                record, etag);
            new AddCdcSinkCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
        
        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}

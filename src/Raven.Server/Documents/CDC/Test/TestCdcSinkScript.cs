using Raven.Client.Documents.Operations.CDC;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.CDC.Test
{
    public class TestCdcSinkScript
    {
        public CdcSinkConfiguration Configuration;

        public string Message { get; set; }
    }
}

using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Agent;

public sealed class AddCollectionArgs
{
    public string? Collection { get; set; }

    public string? Rationale { get; set; }

    public CdcSinkTableConfig? Config { get; set; }
}

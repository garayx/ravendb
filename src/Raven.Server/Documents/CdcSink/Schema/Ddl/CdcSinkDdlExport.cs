using System.Collections.Generic;

namespace Raven.Server.Documents.CdcSink.Schema.Ddl;

internal sealed class CdcSinkDdlExport
{
    public string CatalogName { get; set; }

    public List<CdcSinkDdlFile> Files { get; } = new();
}

internal sealed record CdcSinkDdlFile(string FileName, string Sql);

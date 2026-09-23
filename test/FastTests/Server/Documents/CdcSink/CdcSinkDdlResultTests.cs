using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.CdcSink;

public class CdcSinkDdlResultTests : NoDisposalNeeded
{
    public CdcSinkDdlResultTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void GetFiles_ReturnsEmpty_WhenContentIsNull()
    {
        Assert.Empty(new CdcSinkDdlResult().GetFiles());
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void GetFiles_ReturnsEmpty_WhenContentIsEmpty()
    {
        Assert.Empty(new CdcSinkDdlResult { ZipContent = Array.Empty<byte>() }.GetFiles());
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void GetFiles_ReadsEntriesAndUtf8Content()
    {
        byte[] zip;
        using (var ms = new MemoryStream())
        {
            using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
            {
                using (var writer = new StreamWriter(archive.CreateEntry("public/zażółć.sql").Open(), new UTF8Encoding(false)))
                    writer.Write("CREATE TABLE \"zażółć\" (id int);");
                using (var writer = new StreamWriter(archive.CreateEntry(CdcSinkDdlResult.ForeignKeysFileName).Open(), new UTF8Encoding(false)))
                    writer.Write("ALTER TABLE x ADD CONSTRAINT y FOREIGN KEY (a) REFERENCES z (b);");
            }
            zip = ms.ToArray();
        }

        var files = new CdcSinkDdlResult { ZipContent = zip }.GetFiles();

        Assert.Equal(2, files.Count);
        Assert.Equal("CREATE TABLE \"zażółć\" (id int);", files["public/zażółć.sql"]);
        Assert.StartsWith("ALTER TABLE x", files[CdcSinkDdlResult.ForeignKeysFileName]);
    }
}

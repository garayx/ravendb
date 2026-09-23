using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

internal class CdcSinkDdlResult
{
    public const string ForeignKeysFileName = "zz_foreign_keys.sql";

    public byte[] ZipContent { get; set; }

    public IReadOnlyDictionary<string, string> GetFiles()
    {
        var files = new Dictionary<string, string>(StringComparer.Ordinal);
        if (ZipContent is not { Length: > 0 })
            return files;

        using (var ms = new MemoryStream(ZipContent))
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Read))
        {
            foreach (var entry in archive.Entries)
            {
                using (var reader = new StreamReader(entry.Open(), Encoding.UTF8))
                    files[entry.FullName] = reader.ReadToEnd();
            }
        }

        return files;
    }
}

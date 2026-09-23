using System.Text;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>
/// One table's DDL. Either backed by a file on disk or held in memory after being rendered from a
/// discovered schema; <see cref="OpenRead"/> hides which. <see cref="Content"/> is internal so it
/// does not ride along into the checkpoint document - the DDL is persisted there as an attachment.
/// </summary>
public sealed class SchemaFile
{
    public string Name { get; set; } = string.Empty;

    public string? Path { get; set; }

    public string Digest { get; set; } = string.Empty;

    internal string? Content { get; set; }

    /// <summary>
    /// The caller owns the returned stream. Attachments are queued rather than read when they are
    /// added to a conversation, so it has to stay open until the turn has run.
    /// </summary>
    public Stream OpenRead()
    {
        if (Content is not null)
            return new MemoryStream(Encoding.UTF8.GetBytes(Content));

        if (string.IsNullOrEmpty(Path) == false)
            return File.OpenRead(Path);

        throw new InvalidOperationException(
            $"Schema file '{Name}' has neither in-memory content nor a path on disk.");
    }
}

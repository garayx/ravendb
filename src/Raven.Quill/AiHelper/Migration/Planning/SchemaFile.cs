using System.Text;

namespace Raven.Quill.AiHelper.Migration.Planning;

/// <summary>One table's DDL, rendered from the discovered schema, as the model reads it.</summary>
public sealed record SchemaFile(string Name, string Content)
{
    public Stream OpenRead() => new MemoryStream(Encoding.UTF8.GetBytes(Content));
}

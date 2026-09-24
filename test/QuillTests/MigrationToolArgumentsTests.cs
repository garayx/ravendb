using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Agent;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationToolArgumentsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Well_formed_arguments_are_read()
    {
        const string raw = """
            {"Collection":"Orders","Config":{"CollectionName":"Orders","SourceTableName":"orders",
             "Columns":[{"Column":"id","Name":"Id","Type":"Default"}],"PrimaryKeyColumns":["id"],
             "EmbeddedTables":[{"SourceTableName":"lines","PropertyName":"Lines","Type":"Array"}]}}
            """;

        Assert.True(ToolArguments.TryRead<AddCollectionArgs>(raw, out var args, out _));
        Assert.Equal("Orders", args.Collection);
        Assert.Equal(CdcSinkRelationType.Array, Assert.Single(args.Config!.EmbeddedTables).Type);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_enum_member_that_does_not_exist_is_reported_not_thrown()
    {
        // The observed failure: a relation type given as a column type.
        const string raw = """{"Collection":"Orders","Config":{"Columns":[{"Column":"id","Name":"Id","Type":"Array"}]}}""";

        Assert.False(ToolArguments.TryRead<AddCollectionArgs>(raw, out _, out var error));
        Assert.Contains("Config.Columns[0].Type", error);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("null")]
    [InlineData("{not json")]
    public void Missing_or_broken_arguments_are_reported(string? raw)
    {
        Assert.False(ToolArguments.TryRead<AddCollectionArgs>(raw, out _, out var error));
        Assert.False(string.IsNullOrWhiteSpace(error));
    }
}

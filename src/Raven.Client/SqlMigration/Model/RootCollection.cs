using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class RootCollection : CollectionWithReferences
    {
        public string SourceTableQuery { get; set; }
        public string Patch { get; set; }

        public RootCollection()
        {
        }

        public RootCollection(string sourceTableSchema, string sourceTableName, string name)
            : base(sourceTableSchema, sourceTableName, name)
        {
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SourceTableQuery)] = SourceTableQuery;
            json[nameof(Patch)] = Patch;
            return json;
        }
    }
}

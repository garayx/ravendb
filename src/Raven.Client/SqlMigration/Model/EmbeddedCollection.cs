using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class EmbeddedCollection : CollectionWithReferences, ICollectionReference
    {
        public List<string> JoinColumns { get; set; } = new List<string>();
        public RelationType Type { get; set; }
        public EmbeddedDocumentSqlKeysStorage SqlKeysStorage { get; set; }

        public EmbeddedCollection()
        {
        }

        public EmbeddedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> columns, string name,
            EmbeddedDocumentSqlKeysStorage sqlKeysStorage = EmbeddedDocumentSqlKeysStorage.None)
            : base(sourceTableSchema, sourceTableName, name)
        {
            JoinColumns = columns;
            Type = type;
            SqlKeysStorage = sqlKeysStorage;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(JoinColumns)] = new DynamicJsonArray(JoinColumns);
            json[nameof(Type)] = Type.ToString();
            json[nameof(SqlKeysStorage)] = SqlKeysStorage.ToString();
            json[nameof(LinkedCollections)] = new DynamicJsonArray(LinkedCollections.Select(x => x.ToJson()));
            json[nameof(NestedCollections)] = new DynamicJsonArray(NestedCollections.Select(x => x.ToJson()));
            return json;
        }
    }
}

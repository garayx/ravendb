using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class LinkedCollection : AbstractCollection, ICollectionReference
    {
        public List<string> JoinColumns { get; set; }
        public RelationType Type { get; set; }

        public LinkedCollection()
        {
        }

        public LinkedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> joinColumns, string name)
            : base(sourceTableSchema, sourceTableName, name)
        {
            JoinColumns = joinColumns;
            Type = type;
        }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(JoinColumns)] = new DynamicJsonArray(JoinColumns);
                json[nameof(Type)] = Type.ToString();
                return json;
        }
    }
}

using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public class CollectionWithReferences : AbstractCollection
    {
        public List<EmbeddedCollection> NestedCollections { get; set; } = new List<EmbeddedCollection>();
        public List<LinkedCollection> LinkedCollections { get; set; } = new List<LinkedCollection>();

        public CollectionWithReferences()
        {
        }

        public CollectionWithReferences(string sourceTableSchema, string sourceTableName, string name)
            : base(sourceTableSchema, sourceTableName, name)
        {
        }           

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(NestedCollections)] = new DynamicJsonArray(NestedCollections.Select(x => x.ToJson()));
            json[nameof(LinkedCollections)] = new DynamicJsonArray(LinkedCollections.Select(x => x.ToJson()));
            return json;
        }
    }
}

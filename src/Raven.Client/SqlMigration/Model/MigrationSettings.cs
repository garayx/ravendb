using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    
    public sealed class MigrationSettings : IDynamicJson
    {
        public List<RootCollection> Collections { get; set; }
        public int BatchSize { get; set; } = 1000;
        public int? MaxRowsPerTable { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue();
            json[nameof(Collections)] = new DynamicJsonArray(Collections.Select(x => x.ToJson()));
            json[nameof(BatchSize)] = BatchSize;
            json[nameof(MaxRowsPerTable)] = MaxRowsPerTable;
            return json;
        }
    }
}

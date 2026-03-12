using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public abstract class AbstractCollection : IDynamicJson
    {
        // SQL Schema name
        public string SourceTableSchema { get; set; }

        // SQL Table name
        public string SourceTableName { get; set; }
        
        // RavenDB Collection/Property name
        public string Name { get; set; }
        
        // SQL Column Name -> Document Id Property Name
        public Dictionary<string, string> ColumnsMapping { get; set; }
        
        // SQL Column Name -> Attachment Name
        public Dictionary<string, string> AttachmentNameMapping { get; set; }

        public AbstractCollection()
        {
        }

        protected AbstractCollection(string sourceTableSchema, string sourceTableName, string name)
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            Name = name;
        }

        public virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(SourceTableSchema)] = SourceTableSchema,
                [nameof(SourceTableName)] = SourceTableName,
                [nameof(Name)] = Name,
                [nameof(ColumnsMapping)] = ColumnsMapping.ToJson(),
                [nameof(AttachmentNameMapping)] = AttachmentNameMapping.ToJson()
            };
            return json;
        }


    }
}

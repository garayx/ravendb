using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

internal class CdcSinkDdlRequest : IDynamicJson
{
    public SqlConnectionString Connection { get; set; }

    public string ConnectionStringName { get; set; }

    public string[] Schemas { get; set; }

    public string[] Tables { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Connection)] = Connection?.ToJson(),
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(Schemas)] = Schemas == null ? null : new DynamicJsonArray(Schemas),
            [nameof(Tables)] = Tables == null ? null : new DynamicJsonArray(Tables),
        };
    }
}

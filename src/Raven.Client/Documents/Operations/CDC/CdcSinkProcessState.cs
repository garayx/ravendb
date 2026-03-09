using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CDC;

public class CdcSinkProcessState : IDatabaseTaskStatus
{
    public string NodeTag { get; set; }
    
    public string ConfigurationName { get; set; }

    public string ScriptName { get; set; }
    
    public ulong LastLsn { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(ConfigurationName)] = ConfigurationName,
            [nameof(ScriptName)] = ScriptName,
            [nameof(NodeTag)] = NodeTag,
            [nameof(LastLsn)] = LastLsn
        };

        return json;
    }

    public static string GenerateItemName(string databaseName, string configurationName, string transformationName)
    {
        return $"values/{databaseName}/cdcsink/{configurationName.ToLowerInvariant()}/{transformationName.ToLowerInvariant()}";
    }
}

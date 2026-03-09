using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.QueueSink;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CDC;

public class CdcSinkScript
{



    public string Name { get; set; }
    // TODO: egor link this with RootCollection

    public List<string> Queues { get; set; } = new();

    public string Script { get; set; }
    
    public bool Disabled { get; set; }

    internal CdcSinkConfigurationCompareDifferences Compare(CdcSinkScript script)
    {
        if (script == null)
            throw new ArgumentNullException(nameof(script), "Got null transformation to compare");

        var differences = CdcSinkConfigurationCompareDifferences.None;

        if (script.Queues.Count != Queues.Count)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptsCount;

        var queues = new List<string>(Queues);

        foreach (var queue in script.Queues)
        {
            queues.Remove(queue);
        }

        if (queues.Count != 0)
            differences |= CdcSinkConfigurationCompareDifferences.QueueCount;

        if (script.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptName;

        if (script.Script != Script)
            differences |= CdcSinkConfigurationCompareDifferences.Script;

        if (script.Disabled != Disabled)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptDisabled;

        return differences;
    }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Script)] = Script,
            [nameof(Queues)] = Queues,
        };
    }
}

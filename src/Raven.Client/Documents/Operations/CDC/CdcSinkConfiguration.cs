using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.CDC;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CDC;

/// <summary>
/// The configuration for a queue sink task, which allows integrating with external queueing systems.
/// </summary>
public class CdcSinkConfiguration : IDynamicJson, IDatabaseTask
{
    private bool _initialized;


    [ForceJsonSerialization]
    internal ulong LastLsn { get; set; }
    public MigrationSettings2 Settings { get; set; }

    /// <summary>
    /// Specifies the type of queue broker being used.
    /// </summary>
    public CdcBrokerType BrokerType { get; set; }

    /// <summary>
    /// The unique identifier for the task.
    /// </summary>
    public long TaskId { get; set; }

    /// <summary>
    /// Indicates whether the queue sink task is disabled.
    /// </summary>
    public bool Disabled { get; set; }

    /// <summary>
    /// The name of the queue sink task.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The mentor node assigned to this task, if specified.
    /// </summary>
    public string MentorNode { get; set; }

    /// <summary>
    /// Determines whether the task should be pinned to the mentor node.
    /// </summary>
    public bool PinToMentorNode { get; set; }

    /// <summary>
    /// The name of the connection string used to connect to the queue broker.
    /// </summary>
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// Indicates whether the configuration is running in test mode.
    /// </summary>
    internal bool TestMode { get; set; }

    // TODO: egor link this with MigrationSettings, add BatchSize etc

    /// <summary>
    /// A list of user-defined scripts that process incoming queue messages and define how they should be stored in RavenDB.
    /// </summary>
    public List<CdcSinkScript> Scripts { get; set; } = new();

    [JsonDeserializationIgnore]
    [JsonIgnore]
    internal CdcConnectionString Connection { get; set; }

    public void Initialize(CdcConnectionString connectionString)
    {
        Connection = connectionString;
        _initialized = true;
    }

    public virtual bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && _initialized == false)
            throw new InvalidOperationException("Queue Sink configuration must be initialized");

        errors = new List<string>();

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of Queue Sink configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
            Connection.Validate(errors);

        var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        //if (Scripts.Count == 0)
        //    throw new InvalidOperationException($"'{nameof(Scripts)}' list cannot be empty.");

        //foreach (var script in Scripts)
        //{
        //    if (string.IsNullOrWhiteSpace(script.Script))
        //        errors.Add($"Script '{Name}' must not be empty");

        //    if (uniqueNames.Add(script.Name) == false)
        //        errors.Add($"Script name '{script.Name}' name is already defined. The script names need to be unique");
        //}

        if (Connection != null && BrokerType != Connection.BrokerType)
        {
            errors.Add("Broker type must be the same in the Cdc Sink configuration and in Connection string.");
            return false;
        }


        if (Settings.Collections.Any() == false)
            errors.Add($"{nameof(Settings.Collections)} has no collection to migrate.");
        if (Settings.BatchSize <= 0)
        {
            errors.Add($"{nameof(Settings.BatchSize)} should be greater than 0.");
        }

        return errors.Count == 0;
    }

    public DynamicJsonValue ToJson()
    {
        var result = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(TaskId)] = TaskId,
            [nameof(Disabled)] = Disabled,
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(MentorNode)] = MentorNode,
            [nameof(PinToMentorNode)] = PinToMentorNode,
            [nameof(Scripts)] = new DynamicJsonArray(Scripts.Select(x => x.ToJson())),
            [nameof(BrokerType)] = BrokerType
        };
        result[nameof(LastLsn)] = LastLsn;
        result[nameof(Settings)] = Settings?.ToJson();
        return result;
    }

    public string GetDestination()
    {
        return Connection.GetUrl();
    }

    public ulong GetTaskKey()
    {
        Debug.Assert(TaskId != 0);
        return (ulong)TaskId;
    }

    public string GetMentorNode()
    {
        return MentorNode;
    }

    public string GetDefaultTaskName()
    {
        return $"Queue Sink to {ConnectionStringName}";
    }

    public string GetTaskName()
    {
        return Name;
    }

    public bool IsResourceIntensive()
    {
        return false;
    }

    public bool IsPinnedToMentorNode()
    {
        return PinToMentorNode;
    }

    internal CdcSinkConfigurationCompareDifferences Compare(
        CdcSinkConfiguration config,
        Dictionary<string, CdcConnectionString> connectionStrings,
        List<(string TransformationName, CdcSinkConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config), "Got null config to compare");

        var differences = CdcSinkConfigurationCompareDifferences.None;

        if (config.Scripts.Count != Scripts.Count)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptsCount;

        var localTransforms = Scripts.OrderBy(x => x.Name);
        var remoteTransforms = config.Scripts.OrderBy(x => x.Name);

        using (var localEnum = localTransforms.GetEnumerator())
        using (var remoteEnum = remoteTransforms.GetEnumerator())
        {
            while (localEnum.MoveNext() && remoteEnum.MoveNext())
            {
                var transformationDiff = localEnum.Current.Compare(remoteEnum.Current);
                differences |= transformationDiff;

                if (transformationDiff != CdcSinkConfigurationCompareDifferences.None)
                {
                    transformationDiffs?.Add((localEnum.Current.Name, transformationDiff));
                }
            }
        }

        if (config.ConnectionStringName != ConnectionStringName)
            differences |= CdcSinkConfigurationCompareDifferences.ConnectionStringName;
        else if (config.ConnectionStringName != null)
        {
            var oldConnectionString = Connection;
           CdcConnectionString newConnectionString = null;
            connectionStrings?.TryGetValue(config.ConnectionStringName, out newConnectionString);

            if (newConnectionString == null || oldConnectionString.IsEqual(newConnectionString) == false)
                differences |= CdcSinkConfigurationCompareDifferences.ConnectionString;
        }

        if (config.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationName;

        if (config.MentorNode != MentorNode)
            differences |= CdcSinkConfigurationCompareDifferences.MentorNode;

        if (config.Disabled != Disabled)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationDisabled;

        return differences;
    }
}

public class Collection2 : AbstractCollection
{

    public Collection2() 
    {
    }

    public Collection2(string sourceTableSchema, string sourceTableName, string name) : base(sourceTableSchema, sourceTableName, name)
    {
    }


    public string Patch { get; set; }

    public List<NestedCollection2> NestedCollections { get; set; } = new List<NestedCollection2>();

    public bool InitialLoadCompleted { get; set; }
    public List<string> LastKeyValues { get; set; } = new List<string>();

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Patch)] = Patch;
        json[nameof(NestedCollections)] = new DynamicJsonArray(NestedCollections.Select(x => x.ToJson()));
        json[nameof(InitialLoadCompleted)] = InitialLoadCompleted;
        json[nameof(LastKeyValues)] = new DynamicJsonArray(LastKeyValues);
        return json;
    }
}

/// <summary>
/// Represents a child table that should be embedded as a nested array property
/// inside its parent collection's documents during CDC replication.
/// For example, "productcategory" embedded inside "category" via the "categoryid" join column.
/// </summary>
public class NestedCollection2 : AbstractCollection
{
    public NestedCollection2()
    {
    }

    public NestedCollection2(string sourceTableSchema, string sourceTableName, string name,
        List<string> joinColumns, RelationType type)
        : base(sourceTableSchema, sourceTableName, name)
    {
        JoinColumns = joinColumns;
        Type = type;
    }

    /// <summary>
    /// The FK columns in the child table that reference the parent's PK.
    /// For a OneToMany nested collection, these are the columns in the child table
    /// that correspond to the parent's primary key (e.g., "categoryid" in productcategory).
    /// </summary>
    public List<string> JoinColumns { get; set; } = new List<string>();

    /// <summary>
    /// The relation type — typically OneToMany for nested arrays.
    /// </summary>
    public RelationType Type { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(JoinColumns)] = new DynamicJsonArray(JoinColumns);
        json[nameof(Type)] = Type.ToString();
        return json;
    }
}

public sealed class MigrationSettings2 : IDynamicJson
{
    public List<Collection2> Collections { get; set; }
    public int BatchSize { get; set; } = 1000;
    public int? MaxRowsPerTable { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue();
        json[nameof(Collections)] = new DynamicJsonArray(Collections.Select(x=>x.ToJson()));
        json[nameof(BatchSize)] = BatchSize;
        json[nameof(MaxRowsPerTable)] = MaxRowsPerTable;
        return json;
    }
}

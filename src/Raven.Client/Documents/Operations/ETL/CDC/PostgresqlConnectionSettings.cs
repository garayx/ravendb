using System;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.CDC;

public abstract class SqlConnectionSettings : ISqlConnectionString, IDynamicJson
{
    public abstract string ConnectionString { get; set; }
    public abstract string FactoryName { get; set; }

    public abstract DynamicJsonValue ToJson();
    public abstract DynamicJsonValue ToAuditJson();
}

public sealed class PostgresqlConnectionSettings : SqlConnectionSettings
{
    public override string ConnectionString { get; set; }
    public override string FactoryName { get; set; } = nameof(SqlProvider.Npgsql);

    public string PostgresSlotName { get; set; }
    public string PostgresPublicationName { get; set; }


    public override DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionString)] = ConnectionString,
            [nameof(FactoryName)] = FactoryName,
            [nameof(PostgresSlotName)] = PostgresSlotName,
            [nameof(PostgresPublicationName)] = PostgresPublicationName,

        };
    }

    public override DynamicJsonValue ToAuditJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionString)] = "***",
            [nameof(FactoryName)] = FactoryName,
            [nameof(PostgresSlotName)] = PostgresSlotName,
            [nameof(PostgresPublicationName)] = PostgresPublicationName,
        };
    }

    private bool Equals(PostgresqlConnectionSettings other)
    {
        return string.Equals(ConnectionString, other.ConnectionString, StringComparison.Ordinal) &&
               string.Equals(FactoryName, other.FactoryName, StringComparison.Ordinal) &&
               string.Equals(PostgresSlotName, other.PostgresSlotName, StringComparison.Ordinal) &&
               string.Equals(PostgresPublicationName, other.PostgresPublicationName, StringComparison.Ordinal);
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != GetType())
            return false;
        return Equals((PostgresqlConnectionSettings)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return ((ConnectionString != null ? ConnectionString.GetHashCode() : 0) * 397) ^
                   (FactoryName != null ? FactoryName.GetHashCode() : 0) ^
                   (PostgresSlotName != null ? PostgresSlotName.GetHashCode() : 0) ^
                   (PostgresPublicationName != null ? PostgresPublicationName.GetHashCode() : 0);
        }
    }


    public static bool IsValidPublicationName(string name)
    {
        // 1. Check for null or empty strings
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        // 2. Check the standard PostgreSQL length limit (63 characters)
        if (name.Length > 63)
        {
            return false;
        }

        // 3. Regex to enforce character rules:
        // ^             : Starts at the beginning of the string
        // [a-zA-Z_]     : Must start with a letter or underscore
        // [a-zA-Z0-9_$]*: Subsequent characters can be letters, numbers, underscores, or dollar signs
        // $             : Ends at the string's conclusion
        var regex = new Regex(@"^[a-zA-Z_][a-zA-Z0-9_$]*$");

        return regex.IsMatch(name);
    }
    public static bool IsValidSlotName(string name)
    {
        // 1. Check for null or empty strings
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        // 2. Check the standard PostgreSQL length limit (63 characters)
        if (name.Length > 63)
        {
            return false;
        }

        // 3. Regex to enforce strict slot rules:
        // ^          : Starts at the beginning of the string
        // [a-z0-9_]+ : One or more lowercase letters, numbers, or underscores
        // $          : Ends at the string's conclusion
        var regex = new Regex(@"^[a-z0-9_]+$");

        return regex.IsMatch(name);
    }
}

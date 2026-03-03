using System;
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

    public override DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionString)] = ConnectionString,
            [nameof(FactoryName)] = FactoryName
        };
    }

    public override DynamicJsonValue ToAuditJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionString)] = "***",
            [nameof(FactoryName)] = FactoryName
        };
    }

    private bool Equals(PostgresqlConnectionSettings other)
    {
        return string.Equals(ConnectionString, other.ConnectionString, StringComparison.Ordinal) &&
               string.Equals(FactoryName, other.FactoryName, StringComparison.Ordinal);
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
                   (FactoryName != null ? FactoryName.GetHashCode() : 0);
        }
    }
}

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.CDC;

public sealed class CdcConnectionString : ConnectionString
{
    public CdcBrokerType BrokerType { get; set; }

    public PostgresqlConnectionSettings PostgresqlConnectionSettings { get; set; }

    
    public override ConnectionStringType Type => ConnectionStringType.Cdc;

    protected override void ValidateImpl(List<string> errors)
    {
        switch (BrokerType)
        {
            case CdcBrokerType.PostgreSQL:
                if (PostgresqlConnectionSettings == null || string.IsNullOrWhiteSpace(PostgresqlConnectionSettings.ConnectionString))
                {
                    errors.Add($"{nameof(PostgresqlConnectionSettings)} has no valid setting.");
                }
                break;

            default:
                throw new NotSupportedException($"'{BrokerType}' broker is not supported");
        }
    }

    internal string GetUrl()
    {
        string url;

        switch (BrokerType)
        {
            case CdcBrokerType.PostgreSQL:
                url = PostgresqlConnectionSettings.ConnectionString;
                break;
           
            default:
                throw new NotSupportedException($"'{BrokerType}' broker is not supported");
        }

        return url;
    }
    
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(BrokerType)] = BrokerType;
        json[nameof(PostgresqlConnectionSettings)] = PostgresqlConnectionSettings?.ToJson();
  

        return json;
    }

    public override DynamicJsonValue ToAuditJson()
    {
        var json = base.ToAuditJson();
        
        json[nameof(BrokerType)] = BrokerType;


        return json;
    }

    public override bool IsEqual(ConnectionString connectionString)
    {
        if (connectionString is CdcConnectionString cdcConnectionString)
        {
            var isEqual = base.IsEqual(connectionString);
            if (isEqual == false)
                return false;

            if (BrokerType != cdcConnectionString.BrokerType)
                return false;
            
            switch (BrokerType)
            {
                case CdcBrokerType.PostgreSQL:
                    if (PostgresqlConnectionSettings == null && cdcConnectionString.PostgresqlConnectionSettings == null)
                        return true;
            
                    if (PostgresqlConnectionSettings == null || cdcConnectionString.PostgresqlConnectionSettings == null)
                        return false;
            
                    return PostgresqlConnectionSettings.Equals(cdcConnectionString.PostgresqlConnectionSettings);
            
                default:
                    throw new NotSupportedException($"'{BrokerType}' broker is not supported");
            }
        }

        return false;
    }
}

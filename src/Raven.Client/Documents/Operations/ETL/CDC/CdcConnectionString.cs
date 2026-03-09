using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.CDC;

public sealed class CdcConnectionString : ConnectionString
{
    public CdcBrokerType BrokerType { get; set; }

    // TODO: egor to do this, I need to move all Migration classes from server to client.
    // not migration can be only set up by studio, there is no option to set it up as client operation
    // I wonder, since the setup of table to collection reference is needed for both migration and cdc, maybe we can move this class to some common assembly and use it for both cases, 
    // then we will have this Collections property here and we will be able to use it for both cases.
    // the issue is that it will be hard to set this up without UI.
    // Do we want CDC to be a feature that can be set up in studio only ? 
    //public List<RootCollection> Collections { get; set; }

    public PostgresqlConnectionSettings PostgresqlConnectionSettings { get; set; }

    [ForceJsonSerialization]
    internal ulong LastLsn { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Cdc;

    protected override void ValidateImpl(List<string> errors)
    {
        switch (BrokerType)
        {
            case CdcBrokerType.PostgreSQL:
                if (PostgresqlConnectionSettings == null || string.IsNullOrWhiteSpace(PostgresqlConnectionSettings.ConnectionString))
                    errors.Add($"{nameof(PostgresqlConnectionSettings)} has no valid setting or connection string.");
                else
                {
                    if (PostgresqlConnectionSettings.IsValidPublicationName(PostgresqlConnectionSettings.PostgresPublicationName) == false)
                        errors.Add($"{nameof(PostgresqlConnectionSettings.PostgresPublicationName)} has invalid publication name.");
                    if (PostgresqlConnectionSettings.IsValidSlotName(PostgresqlConnectionSettings.PostgresSlotName) == false)
                        errors.Add($"{nameof(PostgresqlConnectionSettings.PostgresSlotName)} has invalid slot name.");
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
        json[nameof(LastLsn)] = LastLsn;

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

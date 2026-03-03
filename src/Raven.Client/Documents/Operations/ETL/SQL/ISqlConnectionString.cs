using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    public interface ISqlConnectionString
    {
        string ConnectionString { get; set; }
        
        string FactoryName { get; set; }

        //public void ValidateImplMethod(List<string> errors)
        //{
        //    try
        //    {
        //        SqlProviderParser.GetSupportedProvider(FactoryName);
        //    }
        //    catch (NotImplementedException)
        //    {
        //        errors.Add($"Factory '{FactoryName}' is not implemented yet.");
        //    }
        //    catch (Exception)
        //    {
        //        errors.Add($"Unsupported factory '{FactoryName}'");
        //    }

        //    if (string.IsNullOrEmpty(ConnectionString))
        //        errors.Add($"{nameof(ConnectionString)} cannot be empty");
        //}
    }
}

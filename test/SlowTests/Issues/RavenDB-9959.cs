﻿using Tests.Infrastructure;
using System;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9959 : RavenTestBase
    {
        public RavenDB_9959(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Can_use_dollar_args_in_queries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var operation = store
                  .Operations
                  .Send(new PatchByQueryOperation(new IndexQuery
                  {
                      Query = @"
from Employees as e
where e.ReportsTo = $managerId
update {
    e.ManagerName = $managerName
}",
                      QueryParameters = new Parameters
                      {
                          ["managerId"] = "employees/2-A",
                          ["managerName"] = "Jpoh"
                      }
                  },
                  new QueryOperationOptions
                  {
                      AllowStale = false
                  }));

                operation.WaitForCompletion(TimeSpan.FromMinutes(5));
            }
        }
    }
}

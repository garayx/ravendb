﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
    public class RavenDB_17477 : ElasticSearchEtlTestBase
    {
        public RavenDB_17477(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresElasticSearchTheory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task ShouldErrorAndAlertOnInvalidIndexSetupInElastic(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (GetElasticClient(out var client))
            {
                client.Indices.Create(OrdersIndexName, c => c
                    .Map(m => m
                        .Properties(p => p
                            .MatchOnlyText(t => t
                                .Name("Id")))));

                var optChaining = options.JavascriptEngineMode.ToString() == "Jint" ? "" : "?";
                var zeroIfNull = options.JavascriptEngineMode.ToString() == "Jint" ? "" : " ?? 0";
                var config = SetupElasticEtl(store, @$"
var orderData = {{
    Id: id(this),
    OrderLinesCount: this.Lines{optChaining}.length{zeroIfNull},
    TotalCost: 0
}};

loadTo" + OrdersIndexName + @"(orderData);", 
                    new []{ new ElasticSearchIndex { IndexName = OrdersIndexName, DocumentIdProperty = "Id" } },
                    new List<string> { "orders" }, configurationName: "my-etl", transformationName: "my-transformation");

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Lines = new List<OrderLine>() });
                    session.SaveChanges();
                }

                var alert = await AssertWaitForNotNullAsync(() =>
                {
                    TryGetLoadError(store.Database, config, out var error);

                    return Task.FromResult(error);
                }, timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);

                Assert.Contains($"The index '{OrdersIndexName}' has invalid mapping for 'Id' property.", alert.Error);
            }
        }
    }
}

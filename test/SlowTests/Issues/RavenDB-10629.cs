﻿using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10629 : RavenTestBase
    {
        public RavenDB_10629(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void LinqToRQL_CanHandleMemberExpressionWithNullExpression(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var epsilon = TimeSpan.FromSeconds(30);
                    var now = DateTime.UtcNow;

                    var query = (from a in session.Query<Order>()
                                select new
                                {
                                    Date = DateTime.UtcNow
                                }).ToList();

                    Assert.Equal(1, query.Count);
                    Assert.True(query[0].Date - now < epsilon);
                }
            }
        }
    }
}

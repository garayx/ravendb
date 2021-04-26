using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15836 : RavenTestBase
    {
        public RavenDB_15836(ITestOutputHelper output) : base(output)
        {
        }

        private const string DocId = "users/ayende";

        [Fact]
        public void TimeSeriesLinqQuery_CanUseStringInterpolationInName()
        {
            using (var store = GetDocumentStore())
            {
                var timeSeries = "HeartRate";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), DocId);
                    session.TimeSeriesFor(DocId, timeSeries).Append(DateTime.Now, 2);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<User>()
                        .Where(u => u.Id == DocId)
                        .Select(u => RavenQuery.TimeSeries(u, $"'{timeSeries}'").ToList());

                    var result = q.First();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result.Results[0].Value);
                }

            }
        }

        [Fact]
        public async Task TimeSeriesLinqQuery_CanUseSimpleCallExpressionInName()
        {
            using (var store = GetDocumentStore())
            {
                var timeSeries = "HeartRate";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), DocId);
                    var timeSeriesFor = session.TimeSeriesFor(DocId, timeSeries);
                    timeSeriesFor.Append(DateTime.MinValue.AddMilliseconds(1), 0, "watches/fitbit");
                    session.SaveChanges();
                }
            }
        }


        [Fact]
        public void TimeSeriesLinqQuery_CanUseSimpleCallExpressionInGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var timeSeries = "HeartRate";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), DocId);
                    session.TimeSeriesFor(DocId, timeSeries).Append(DateTime.Now, 2);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var days = 1;
                    var q = session.Query<User>()
                        .Where(u => u.Id == DocId)
                        .Select(u => RavenQuery.TimeSeries(u, timeSeries)
                            .GroupBy(GetGroupBy(days))
                            .ToList());

                    var result = q.First();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result.Results[0].Min[0]);
                }

            }
        }

        private string GetGroupBy(int days)
        {
            return $"{days} days";
        }
    }
}

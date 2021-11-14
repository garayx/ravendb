using System;
using FastTests;
using Tests.Infrastructure.ConnectionString;
using xRetry;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchRetryTheoryAttribute : RetryTheoryAttribute
    {
        private static readonly bool _canConnect;

        static RequiresElasticSearchRetryTheoryAttribute()
        {
            _canConnect = ElasticSearchTestNodes.Instance.CanConnect;
        }

        public RequiresElasticSearchRetryTheoryAttribute(int maxRetries = 3,
            int delayBetweenRetriesMs = 1000,
            params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}

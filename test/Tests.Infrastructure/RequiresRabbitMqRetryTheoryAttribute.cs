using System;
using FastTests;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresRabbitMqRetryTheoryAttribute : RetryTheoryAttribute
{
    internal static readonly bool CanConnect;

    static RequiresRabbitMqRetryTheoryAttribute()
    {
        CanConnect = RabbitMqConnectionString.Instance.CanConnect;
    }

    public RequiresRabbitMqRetryTheoryAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (CanConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}

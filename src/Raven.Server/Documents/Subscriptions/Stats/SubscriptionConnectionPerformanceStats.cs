// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Subscriptions;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionPerformanceStats
    {        
        public long ConnectionId { get; set; }
        public string ClientUri { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }

        public long BatchCount { get; set; }
        public long TotalBatchSizeInBytes { get; set; }

        public string Exception { get; set; }
        public SubscriptionConnectionBase.SubscriptionError? ErrorType { get; set; }

        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
        
        public double DurationInMs { get; }
        
        public SubscriptionConnectionPerformanceOperation Details { get; set; }
        
        public SubscriptionConnectionPerformanceStats(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }
    }
}

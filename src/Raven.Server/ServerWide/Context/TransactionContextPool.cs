// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionContextPool : JsonContextPoolBase<TransactionOperationContext>, ITransactionContextPool<TransactionOperationContext>
    {
        private StorageEnvironment _storageEnvironment;

        public TransactionContextPool(StorageEnvironment storageEnvironment, Size? maxContextSizeToKeepInMb = null, Logger logger = null) : base(maxContextSizeToKeepInMb, logger)
        {
            _storageEnvironment = storageEnvironment;
        }

        protected override TransactionOperationContext CreateContext()
        {
            int initialSize;
            int longLivedSize;
            int maxNumberOfAllocatedStringValues;
            if (_storageEnvironment.Options.RunningOn32Bits)
            {
                initialSize = 4096;
                longLivedSize = 4 * 1024;
                maxNumberOfAllocatedStringValues = 2 * 1024;
            }
            else
            {
                initialSize = 32 * 1024;
                longLivedSize = 16 * 1024;
                maxNumberOfAllocatedStringValues = 8 * 1024;
            }

            return new TransactionOperationContext(_storageEnvironment, initialSize, longLivedSize, maxNumberOfAllocatedStringValues, LowMemoryFlag, _logger.GetLoggerFor(nameof(RavenTransaction), _logger.Type));
        }

        public override void Dispose()
        {
            _storageEnvironment = null;
            base.Dispose();
        }
    }
}

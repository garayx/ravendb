using Sparrow.Logging;
using Sparrow.Platform;

namespace Sparrow.Json
{
    public class JsonContextPool : JsonContextPoolBase<JsonOperationContext>
    {
        private readonly int _maxNumberOfAllocatedStringValuesPerContext;

        public JsonContextPool(Logger logger = null) : base(logger)
        {
        }

        public JsonContextPool(Size? maxContextSizeToKeep, Logger logger = null)
            : this(maxContextSizeToKeep, null, PlatformDetails.Is32Bits == false ? 8 * 1024 : 2 * 1024, logger)
        {
        }

        internal JsonContextPool(Size? maxContextSizeToKeep, long? maxNumberOfContextsToKeepInGlobalStack, int maxNumberOfAllocatedStringValuesPerContext, Logger logger = null)
            : base(maxContextSizeToKeep, maxNumberOfContextsToKeepInGlobalStack, logger)
        {
            _maxNumberOfAllocatedStringValuesPerContext = maxNumberOfAllocatedStringValuesPerContext;
        }

        protected override JsonOperationContext CreateContext()
        {
            if (PlatformDetails.Is32Bits)
                return new JsonOperationContext(4096, 16 * 1024, _maxNumberOfAllocatedStringValuesPerContext, LowMemoryFlag);

            return new JsonOperationContext(32 * 1024, 16 * 1024, _maxNumberOfAllocatedStringValuesPerContext, LowMemoryFlag);
        }
    }
}

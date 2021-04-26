using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    public class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(Logger logger) : base(logger)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }
    }
}

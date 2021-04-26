using System.Buffers;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    public class LowMemoryMonitor : AbstractLowMemoryMonitor
    {
        private readonly SmapsReader _smapsReader;

        private byte[][] _buffers;
        private readonly Logger _logger;

        public LowMemoryMonitor(Logger logger)
        {
            if (PlatformDetails.RunningOnLinux)
            {
                var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                _buffers = new[] { buffer1, buffer2 };
                _smapsReader = new SmapsReader(_buffers);
            }

            _logger = logger;
        }

        public override MemoryInfoResult GetMemoryInfoOnce()
        {
            return MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader(_logger);
        }

        public override MemoryInfoResult GetMemoryInfo(bool extended = false)
        {
            return MemoryInformation.GetMemoryInfo(_logger, extended ? _smapsReader : null, extended: extended);
        }

        public override bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            return MemoryInformation.IsEarlyOutOfMemory(memInfo, out commitChargeThreshold);
        }

        public override DirtyMemoryState GetDirtyMemoryState()
        {
            return MemoryInformation.GetDirtyMemoryState();
        }

        public override void AssertNotAboutToRunOutOfMemory()
        {
            MemoryInformation.AssertNotAboutToRunOutOfMemory(_logger);
        }

        public override void Dispose()
        {
            if (_buffers != null)
            {
                ArrayPool<byte>.Shared.Return(_buffers[0]);
                ArrayPool<byte>.Shared.Return(_buffers[1]);

                _buffers = null;
            }
        }
    }
}

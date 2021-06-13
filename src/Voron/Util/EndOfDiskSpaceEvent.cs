// -----------------------------------------------------------------------
//  <copyright file="EndOfDiskSpaceEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.ExceptionServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Voron.Util
{
    public class EndOfDiskSpaceEvent
    {
        private readonly long _availableSpaceWhenEventOccurred;
        private readonly string _path;
        private readonly ExceptionDispatchInfo _edi;
        private Logger _logger;

        public EndOfDiskSpaceEvent(string path, long availableSpaceWhenEventOccurred, ExceptionDispatchInfo edi, Logger logger)
        {
            _availableSpaceWhenEventOccurred = availableSpaceWhenEventOccurred;
            _path = path;
            _edi = edi;
            _logger = logger;
        }

        public void AssertCanContinueWriting()
        {
            var diskInfoResult = DiskSpaceChecker.GetDiskSpaceInfo(_path);
            if (diskInfoResult == null)
                return;

            var freeSpaceInBytes = diskInfoResult.TotalFreeSpace.GetValue(SizeUnit.Bytes);
            if (freeSpaceInBytes > _availableSpaceWhenEventOccurred)
                return;

            _edi.Throw();
        }
    }
}

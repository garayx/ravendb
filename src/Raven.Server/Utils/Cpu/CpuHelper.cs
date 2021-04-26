using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Dashboard;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cpu
{
    public static class CpuHelper
    {
        internal static Logger _logger;

        internal static ICpuUsageCalculator GetOSCpuUsageCalculator(Logger logger = null)
        {
            _logger = logger;

            ICpuUsageCalculator calculator;
            if (PlatformDetails.RunningOnPosix == false)
            {
                calculator = new WindowsCpuUsageCalculator();
            }
            else if (PlatformDetails.RunningOnMacOsx)
            {
                calculator = new MacInfoCpuUsageCalculator();
            }
            else
            {
                calculator = new LinuxCpuUsageCalculator();
            }
            //    calculator.Init(); // TODO: remove called in RavenServer.Initialize()
            return calculator;
        }

        internal static ExtensionPointCpuUsageCalculator GetExtensionPointCpuUsageCalculator(
            JsonContextPool contextPool,
            MonitoringConfiguration configuration,
            NotificationCenter.NotificationCenter notificationCenter,
            Logger logger)
        {
            _logger = logger;
            var extensionPoint = new ExtensionPointCpuUsageCalculator(
                contextPool,
                configuration.CpuUsageMonitorExec,
                configuration.CpuUsageMonitorExecArguments,
                notificationCenter,
                _logger);

            return extensionPoint;
        }

        public static long GetNumberOfActiveCores(Process process)
        {
            try
            {
                return Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
            }
            catch (NotSupportedException)
            {
                return ProcessorInfo.ProcessorCount;
            }
            catch (Exception e)
            {
                if (_logger?.IsInfoEnabled == true)
                    _logger.Info("Failure to get the number of active cores", e);

                return ProcessorInfo.ProcessorCount;
            }
        }

        public static (long TotalProcessorTimeTicks, long TimeTicks) GetProcessTimes(Process process)
        {
            try
            {
                var timeTicks = SystemTime.UtcNow.Ticks;
                var totalProcessorTime = process.TotalProcessorTime.Ticks;
                return (TotalProcessorTimeTicks: totalProcessorTime, TimeTicks: timeTicks);
            }
            catch (NotSupportedException)
            {
                return (0, 0);
            }
            catch (Exception e)
            {
                if (_logger?.IsInfoEnabled == true)
                    _logger.Info($"Failure to get process times, error: {e.Message}", e);

                return (0, 0);
            }
        }
    }
}

using System;
using DasMulli.Win32.ServiceUtils;
using Raven.Server.Config;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public static class WindowsServiceRunner
    {
        public static void Run(string serviceName, RavenConfiguration configuration, string[] args, Logger logger)
        {
            var service = new RavenWin32Service(serviceName, configuration, args, logger);
            Program.RestartServer = service.Restart;
            var serviceHost = new Win32ServiceHost(service);
            serviceHost.Run();
        }

        public static bool ShouldRunAsWindowsService()
        {
            if (PlatformDetails.RunningOnPosix)
                return false;

            using (var p = ParentProcessUtilities.GetParentProcess())
            {
                if (p == null)
                    return false;
                var hasBeenStartedByServices = p.ProcessName == "services";
                return hasBeenStartedByServices;
            }
        }
    }

    internal class RavenWin32Service : IWin32Service
    {
        private static Logger _logger;

        private RavenServer _ravenServer;

        private readonly string[] _args;

        public string ServiceName { get; }

        private ServiceStoppedCallback _serviceStoppedCallback;

        public RavenWin32Service(string serviceName, RavenConfiguration configuration, string[] args, Logger logger)
        {
            ServiceName = serviceName;
            _args = args;
            _logger = logger;
            _ravenServer = new RavenServer(configuration, logger);
        }

        public void Start(string[] startupArguments, ServiceStoppedCallback serviceStoppedCallback)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Starting RavenDB Windows Service: {ServiceName}.");

            _serviceStoppedCallback = serviceStoppedCallback;

            try
            {
                _ravenServer.OpenPipes();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Unable to OpenPipe. Admin Channel will not be available to the user", e);

                throw;
            }

            try
            {
                _ravenServer.Initialize();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error initializing the server", e);

                throw;
            }
        }

        public void Restart()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Restarting RavenDB Windows Service: {ServiceName}.");

            _ravenServer.Dispose();
            var configuration = RavenConfiguration.CreateForServer(null, CommandLineSwitches.CustomConfigPath);

            if (_args != null)
                configuration.AddCommandLine(_args);

            configuration.Initialize();
            _ravenServer = new RavenServer(configuration, _logger);
            Start(_args, _serviceStoppedCallback);

            configuration.Initialize();
        }

        public void Stop()
        {
            if (_logger.IsOperationsEnabled)
                _logger.OperationsWithWait($"Stopping RavenDB Windows Service: {ServiceName}.").Wait(TimeSpan.FromSeconds(15));

            _ravenServer.Dispose();
            _serviceStoppedCallback();
        }
    }
}

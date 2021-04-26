using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Dashboard
{
    public class ServerDashboardNotifications : NotificationsBase
    {
        public ServerDashboardNotifications(ServerStore serverStore, CancellationToken shutdown)
        {
            var options = new ServerDashboardOptions();

            var machineResourcesNotificationSender = 
                new MachineResourcesNotificationSender(serverStore.Server, Watchers, options.MachineResourcesThrottle, shutdown, serverStore.Logger.GetLoggerFor(nameof(MachineResourcesNotificationSender), LogType.Server));
            BackgroundWorkers.Add(machineResourcesNotificationSender);

            var databasesInfoNotificationSender = 
                new DatabasesInfoNotificationSender(serverStore, Watchers, options.DatabasesInfoThrottle, shutdown, serverStore.Logger.GetLoggerFor(nameof(DatabasesInfoNotificationSender), LogType.Server));
            BackgroundWorkers.Add(databasesInfoNotificationSender);
        }
    }
}

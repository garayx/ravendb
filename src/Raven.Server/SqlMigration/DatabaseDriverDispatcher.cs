﻿using System;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.MySQL;
using Raven.Server.SqlMigration.NpgSQL;

namespace Raven.Server.SqlMigration
{
    public static class DatabaseDriverDispatcher
    {
        public static IDatabaseDriver CreateDriver(MigrationProvider provider, string connectionString)
        {
            switch (provider)
            {
                case MigrationProvider.MsSQL:
                    return new MsSqlDatabaseMigrator(connectionString);
                
                case MigrationProvider.MySQL:
                    return new MySqlDatabaseMigrator(connectionString);

                case MigrationProvider.NpgSQL:
                    return new NpgSqlDatabaseMigrator(connectionString);

                default:
                    throw new InvalidOperationException("Provider " + provider + " is not yet supported");
            }
        }
    }
}

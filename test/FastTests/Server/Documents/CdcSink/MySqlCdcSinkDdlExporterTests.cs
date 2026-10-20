using Raven.Server.Documents.CdcSink.Schema.Ddl;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.CdcSink;

public class MySqlCdcSinkDdlExporterTests : NoDisposalNeeded
{
    public MySqlCdcSinkDdlExporterTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RemoveForeignKeys_DropsTrailingConstraintsAndFixesComma()
    {
        const string ddl = "CREATE TABLE `orders` (\n" +
                           "  `id` int NOT NULL,\n" +
                           "  `customer_id` int NOT NULL,\n" +
                           "  PRIMARY KEY (`id`),\n" +
                           "  KEY `fk_orders_customer` (`customer_id`),\n" +
                           "  CONSTRAINT `chk_id` CHECK ((`id` > 0)),\n" +
                           "  CONSTRAINT `fk_orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE,\n" +
                           "  CONSTRAINT `fk``odd` FOREIGN KEY (`id`) REFERENCES `orders` (`id`)\n" +
                           ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        var result = MySqlCdcSinkDdlExporter.RemoveForeignKeys(ddl);

        Assert.Equal("CREATE TABLE `orders` (\n" +
                     "  `id` int NOT NULL,\n" +
                     "  `customer_id` int NOT NULL,\n" +
                     "  PRIMARY KEY (`id`),\n" +
                     "  KEY `fk_orders_customer` (`customer_id`),\n" +
                     "  CONSTRAINT `chk_id` CHECK ((`id` > 0))\n" +
                     ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", result);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void RemoveForeignKeys_LeavesTablesWithoutForeignKeysUntouched()
    {
        const string ddl = "CREATE TABLE `customers` (\n" +
                           "  `id` int NOT NULL AUTO_INCREMENT,\n" +
                           "  PRIMARY KEY (`id`)\n" +
                           ") ENGINE=InnoDB";

        Assert.Equal(ddl, MySqlCdcSinkDdlExporter.RemoveForeignKeys(ddl));
    }
}

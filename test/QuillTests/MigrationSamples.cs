using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;

namespace QuillTests;

/// <summary>Builders for the migration-agent tests: a catalog and a configuration that validates.</summary>
public static class MigrationSamples
{
    public static SchemaCatalog Catalog(params (string Schema, string Table, string[] Columns)[] tables)
    {
        var schema = new CdcSinkSourceSchema();

        foreach (var (tableSchema, table, columns) in tables)
        {
            schema.Tables.Add(new CdcSinkSourceTable
            {
                SourceTableSchema = tableSchema,
                SourceTableName = table,
                Columns = columns
                    .Select(c => new CdcSinkSourceColumn { Name = c, NativeType = "text" })
                    .ToList()
            });
        }

        return SchemaCatalog.FromDiscoveredSchema(schema);
    }

    /// <summary>The orders/order_lines/customers shape the agent's worked example teaches.</summary>
    public static SchemaCatalog OrdersCatalog() => Catalog(
        ("public", "orders", ["order_id", "ordered_at", "customer_id"]),
        ("public", "order_lines", ["order_line_id", "order_id", "quantity", "unit_price"]),
        ("public", "customers", ["customer_id", "name"]));

    public static CdcSinkTableConfig ValidOrders() => new()
    {
        CollectionName = "Orders",
        SourceTableSchema = "public",
        SourceTableName = "orders",
        PrimaryKeyColumns = ["order_id"],
        Columns =
        [
            new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
            new CdcColumnMapping { Column = "ordered_at", Name = "OrderedAt" }
        ]
    };

    public static CdcSinkEmbeddedTableConfig ValidLines() => new()
    {
        SourceTableSchema = "public",
        SourceTableName = "order_lines",
        PropertyName = "Lines",
        Type = CdcSinkRelationType.Array,
        JoinColumns = ["order_id"],
        PrimaryKeyColumns = ["order_line_id"],
        Columns =
        [
            new CdcColumnMapping { Column = "order_line_id", Name = "LineId" },
            new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
        ]
    };

    public static CdcSinkLinkedTableConfig ValidCustomer() => new()
    {
        SourceTableSchema = "public",
        SourceTableName = "customers",
        PropertyName = "Customer",
        LinkedCollectionName = "Customers",
        JoinColumns = ["customer_id"]
    };
}

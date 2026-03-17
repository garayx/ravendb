namespace SlowTests.Server.Documents.CDC
{
    public class Nopktable
    {
        public string Id { get; set; }
    }

    public class Unsupportedtable
    {
    }

    public class Customer
    {
        public string Firstname { get; set; }
    }

    public class Category
    {
        public string Name { get; set; }
    }

    public class Order
    {
        public string Orderdate { get; set; }
        public decimal Totalamount { get; set; }
    }

    public class Orderitem
    {
        public decimal Unitprice { get; set; }
    }

    public class Details
    {
        public string Name { get; set; }
    }

    public class Product
    {
        public decimal Unitprice { get; set; }
        public bool Isdiscontinued { get; set; }
    }

    public class Photo
    {
    }
}

namespace Sparrow.Logging
{
    public class LoggingSourceHolder
    {
        public string Source { get; set; }
        public string Type { get; set; }
        public Logger Logger { get; set; }
        public LogMode Mode { get; set; }
    }

    public class LoggerHolder
    {
        public string Name { set; get; }
        public LogMode Mode { set; get; }
        public LogType Type { set; get; }
    }
}

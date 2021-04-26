using System;

namespace Sparrow.Logging
{
    [Flags]
    public enum LogMode
    {
        None = 0,
        Operations = 1, // High level info for operational users
        Information = 3 // Low level debug info
    }

    [Flags]
    public enum LogType
    {
        //TODO: put Invalid type as first ?
        Server = 0,
        Database = 1,
        Cluster = 3,
        Client = 4,
        Index = 5,

        Instance = 6 // TODO: maybe put it to high number ? 
    }
}

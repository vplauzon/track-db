using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    [Flags]
    internal enum DataManagementActivity
    {
        None = 0,
        MergeAllInMemoryLogs = 1,
        PersistAllData = 2
    }
}
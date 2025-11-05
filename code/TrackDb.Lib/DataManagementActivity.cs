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
        PersistAllNonMetaData = 2,
        /// <summary>We limit to 1st level metadata to avoid recursive explosion of metadata tables.</summary>
        PersistAllMetaDataFirstLevel = 4,
        HardDeleteAll = 8
    }
}
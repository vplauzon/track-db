using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record TransactionContent(
        Dictionary<string, TableTransactionContent> Tables,
        Dictionary<string, List<long>>? Tombstones);
}
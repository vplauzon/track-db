using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record TransactionContent(
        List<TableTransactionContent> Tables,
        List<TombstoneContent> Tombstones);
}
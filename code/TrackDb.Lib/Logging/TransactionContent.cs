using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record TransactionContent(
        IImmutableDictionary<string, TableTransactionContent> Tables,
        IImmutableDictionary<string, List<long>> Tombstones);
}
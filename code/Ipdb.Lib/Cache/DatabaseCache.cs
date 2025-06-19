using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Cache
{
    internal record DatabaseCache(
        IImmutableList<ImmutableTransactionLog> TransactionLogs,
        DocumentBlockCollection DocumentBlocks,
        IndexBlockCollection IndexBlocks)
    {
        public int GetTransactionLogsDocumentSize()
        {
            return TransactionLogs.Sum(t => t.GetDocumentSize());
        }

        public int GetTransactionLogsItemCount()
        {
            return TransactionLogs.Sum(t => t.GetItemCount());
        }
    }
}
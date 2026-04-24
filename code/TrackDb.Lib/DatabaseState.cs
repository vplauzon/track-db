using TrackDb.Lib.InMemory;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Frozen;

namespace TrackDb.Lib
{
    /// <summary>
    /// Whole state for a database, including all in-memory data in the "main branch"
    /// and each transaction.
    /// </summary>
    /// <param name="InMemoryDatabase"></param>
    /// <param name="TableMap"></param>
    internal record DatabaseState(
        InMemoryDatabase InMemoryDatabase,
        FrozenDictionary<string, TableProperties> TableMap,
        long AppendRecordCount,
        long TombstoneRecordCount,
        ReversedLinkedList<TransactionLogItem>? TransactionLogItems)
    {
        public DatabaseState(FrozenDictionary<string, TableProperties> tableMap)
            : this(new InMemoryDatabase(), tableMap, 0, 0, null)
        {
        }
    }
}
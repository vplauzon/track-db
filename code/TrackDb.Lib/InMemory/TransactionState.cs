using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.InMemory
{
    internal record TransactionState(
        InMemoryDatabase InMemoryDatabase,
        TransactionLog UncommittedTransactionLog)
    {
        public TransactionState(InMemoryDatabase inMemoryDatabase)
            : this(inMemoryDatabase, new TransactionLog())
        {
        }

        public IEnumerable<IBlock> ListTransactionLogBlocks(string tableName)
        {
            if (UncommittedTransactionLog
                .TableBlockBuilderMap.TryGetValue(tableName, out var ubb))
            {
                yield return ubb;
            }
            if (InMemoryDatabase.TableTransactionLogsMap.TryGetValue(tableName, out var logs))
            {
                foreach (var block in logs.InMemoryBlocks)
                {
                    yield return block;
                }
            }
        }
    }
}
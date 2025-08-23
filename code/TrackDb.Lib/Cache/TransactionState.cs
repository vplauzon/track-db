using TrackDb.Lib.Cache.CachedBlock;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Cache
{
    internal record TransactionState(
        DatabaseCache DatabaseCache,
        TransactionLog UncommittedTransactionLog)
    {
        public IEnumerable<IBlock> ListTransactionLogBlocks(string tableName)
        {
            if (UncommittedTransactionLog
                .TableBlockBuilderMap.TryGetValue(tableName, out var ubb))
            {
                yield return ubb;
            }
            if (DatabaseCache.TableTransactionLogsMap.TryGetValue(tableName, out var logs))
            {
                foreach (var block in logs.InMemoryBlocks)
                {
                    yield return block;
                }
            }
        }
    }
}
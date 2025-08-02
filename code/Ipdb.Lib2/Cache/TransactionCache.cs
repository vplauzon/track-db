using Ipdb.Lib2.Cache.CachedBlock;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache
{
    internal record TransactionCache(
        DatabaseCache DatabaseCache,
        TransactionLog UncommittedTransactionLog)
    {
        public IEnumerable<IBlock> ListTransactionLogBlocks(string tableName)
        {
            if (UncommittedTransactionLog
                .TableTransactionLogMap.TryGetValue(tableName, out var ul))
            {
                yield return ul.BlockBuilder;
            }
            if (DatabaseCache.TableTransactionLogs.TryGetValue(tableName, out var logs))
            {
                foreach (var log in logs)
                {
                    yield return log.InMemoryBlock;
                }
            }
        }

        public IEnumerable<long> ListDeletedRecordIds(string tableName)
        {
            if (UncommittedTransactionLog
                .TableTransactionLogMap.TryGetValue(tableName, out var ul))
            {
                foreach (var id in ul.DeletedRecordIds)
                {
                    yield return id;
                }
            }
            if (DatabaseCache.TableTransactionLogs.TryGetValue(tableName, out var logs))
            {
                foreach (var log in logs)
                {
                    foreach (var id in log.DeletedRecordIds)
                    {
                        yield return id;
                    }
                }
            }
        }
    }
}
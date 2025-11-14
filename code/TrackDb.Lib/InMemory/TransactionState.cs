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

        public IEnumerable<IBlock> ListBlocks(string tableName)
        {
            var doOverrideCommitted = false;

            if (UncommittedTransactionLog
                .TransactionTableLogMap
                .TryGetValue(tableName, out var ttl))
            {
                yield return ttl.NewDataBlockBuilder;
                if (ttl.CommittedDataBlock != null)
                {
                    doOverrideCommitted = true;
                    yield return ttl.CommittedDataBlock;
                }
            }
            if (!doOverrideCommitted
                && InMemoryDatabase.TableTransactionLogsMap.TryGetValue(tableName, out var logs))
            {
                foreach (var block in logs.InMemoryBlocks)
                {
                    yield return block;
                }
            }
        }

        public void LoadCommittedTransactionLogs(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
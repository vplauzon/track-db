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
                yield return ttl.NewDataBlock;
                if (ttl.CommittedDataBlock != null)
                {
                    doOverrideCommitted = true;
                    yield return ttl.CommittedDataBlock;
                }
            }
            if (!doOverrideCommitted
                && InMemoryDatabase.TransactionTableLogsMap.TryGetValue(tableName, out var logs))
            {
                foreach (var block in logs.InMemoryBlocks)
                {
                    yield return block;
                }
            }
        }

        #region LoadCommittedBlocks
        /// <summary>
        /// Load all committed transaction logs of a table and stores it in
        /// <see cref="TransactionTableLog.CommittedDataBlock"/>.
        /// If the table is already loaded, nothing happens.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns><c>true</c> iif something was loaded.</returns>
        public bool LoadCommittedBlocksInTransaction(string tableName)
        {
            var committedTxTableLogsMap = InMemoryDatabase.TransactionTableLogsMap;
            var uncommittedTxTableLogMap = UncommittedTransactionLog.TransactionTableLogMap;

            if (committedTxTableLogsMap.TryGetValue(tableName, out var committedTxTableLogs))
            {
                if (!uncommittedTxTableLogMap.TryGetValue(tableName, out var uncommittedTxTableLog)
                    //  Let's check it's not already loaded
                    || uncommittedTxTableLog.CommittedDataBlock == null)
                {
                    var committedDataBlock = committedTxTableLogs.MergeLogs();

                    if (uncommittedTxTableLog != null)
                    {
                        uncommittedTxTableLogMap[tableName] = new TransactionTableLog(
                            uncommittedTxTableLog.NewDataBlock,
                            committedDataBlock);
                    }
                    else
                    {
                        uncommittedTxTableLogMap[tableName] = new TransactionTableLog(
                            ((IBlock)committedDataBlock).TableSchema,
                            committedDataBlock);
                    }

                    return true;
                }
            }

            return false;
        }
        #endregion
    }
}
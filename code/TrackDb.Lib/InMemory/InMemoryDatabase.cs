using System;
using System.Collections;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.InMemory
{
    internal record InMemoryDatabase(
        FrozenDictionary<string, ImmutableTableTransactionLogs> TransactionTableLogsMap,
        FrozenDictionary<int, BlockTombstones> BlockTombstonesIndex,
        FrozenDictionary<BlockAvailability, FrozenDictionary<int, AvailableBlock>> AvailableBlockIndex)
    {
        public InMemoryDatabase()
            : this(
                  FrozenDictionary<string, ImmutableTableTransactionLogs>.Empty,
                  FrozenDictionary<int, BlockTombstones>.Empty,
                  FrozenDictionary<BlockAvailability, FrozenDictionary<int, AvailableBlock>>.Empty)
        {
        }

        public int GetMaxInMemoryBlocksPerTable()
        {
            return TransactionTableLogsMap.Any()
                ? TransactionTableLogsMap
                .Max(p => p.Value.InMemoryBlocks.Count)
                : 0;
        }

        public IEnumerable<(string TableName, int RecordCount)> GetTotalInMemoryDataRecordsByTable()
        {
            return TransactionTableLogsMap
                .Select(p => (p.Key, p.Value.InMemoryBlocks.Sum(b => b.RecordCount)));
        }

        public InMemoryDatabase CommitLog(TransactionState transactionState)
        {
            //  Copy of current in-memory state
            var logMap = TransactionTableLogsMap.ToDictionary();

            //  Loop through the transaction data (tables)
            foreach (var pair in transactionState.UncommittedTransactionLog.TransactionTableLogMap)
            {
                var tableName = pair.Key;
                var txTableLog = pair.Value;
                var inMemoryBlocks = ImmutableArray<IBlock>.Empty.ToBuilder();

                //  First copy what's currently in db
                if (logMap.ContainsKey(tableName) && txTableLog.ReplacingDataBlock == null)
                {
                    inMemoryBlocks.AddRange(logMap[tableName].InMemoryBlocks);
                }
                //  Replace committed
                if (txTableLog.ReplacingDataBlock != null)
                {
                    var transactionTableLogsMap = transactionState
                        .InMemoryDatabase
                        .TransactionTableLogsMap;
                    var inTransactionCommittedCount = transactionTableLogsMap.ContainsKey(tableName)
                        ? transactionTableLogsMap[tableName].InMemoryBlocks.Count
                        : 0;

                    if (inTransactionCommittedCount > 0)
                    {
                        inMemoryBlocks.AddRange(logMap[tableName].InMemoryBlocks
                            .Skip(inTransactionCommittedCount));
                    }
                    if (((IBlock)txTableLog.ReplacingDataBlock).RecordCount != 0)
                    {
                        inMemoryBlocks.Add(txTableLog.ReplacingDataBlock);
                    }
                }

                //  New data
                if (((IBlock)txTableLog.NewDataBlock).RecordCount != 0)
                {
                    inMemoryBlocks.Add(txTableLog.NewDataBlock);
                }
                if (inMemoryBlocks.Count > 0)
                {
                    logMap[tableName] =
                        new ImmutableTableTransactionLogs(inMemoryBlocks.ToImmutable());
                }
                else if (logMap.ContainsKey(tableName))
                {   //  The transaction emptied the blocks
                    logMap.Remove(tableName);
                }
            }

            var replacingBlockTombstonesIndex =
                transactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex;
            var replacingAvailableBlockIndex =
                transactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex;

            return new InMemoryDatabase(
                logMap.ToFrozenDictionary(),
                replacingBlockTombstonesIndex?.ToFrozenDictionary() ?? BlockTombstonesIndex,
                replacingAvailableBlockIndex?.ToFrozenDictionary(p => p.Key, p => p.Value.ToFrozenDictionary())
                ?? AvailableBlockIndex);
        }
    }
}
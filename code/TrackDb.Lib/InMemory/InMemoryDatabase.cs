using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.InMemory
{
    internal record InMemoryDatabase(
        IImmutableDictionary<string, ImmutableTableTransactionLogs> TransactionTableLogsMap)
    {
        public InMemoryDatabase()
            : this(ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty)
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
            var logMap = ImmutableDictionary<string, ImmutableTableTransactionLogs>
                .Empty
                .ToBuilder();

            //  Copy of current in-memory state
            logMap.AddRange(TransactionTableLogsMap);
            //  Loop through the transaction data (tables)
            foreach (var pair in transactionState.UncommittedTransactionLog.TransactionTableLogMap)
            {
                var tableName = pair.Key;
                var txTableLog = pair.Value;
                var inMemoryBlocks = ImmutableArray<IBlock>.Empty.ToBuilder();

                //  First copy what's currently in db
                if (logMap.ContainsKey(tableName) && txTableLog.CommittedDataBlock == null)
                {
                    inMemoryBlocks.AddRange(logMap[tableName].InMemoryBlocks);
                }
                //  Replace committed
                if (txTableLog.CommittedDataBlock != null)
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
                    if (((IBlock)txTableLog.CommittedDataBlock).RecordCount != 0)
                    {
                        inMemoryBlocks.Add(txTableLog.CommittedDataBlock);
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

            return new InMemoryDatabase(logMap.ToImmutable());
        }
    }
}
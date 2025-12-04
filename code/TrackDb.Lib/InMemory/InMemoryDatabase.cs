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
            var logs = ImmutableDictionary<string, ImmutableTableTransactionLogs>
                .Empty
                .ToBuilder();

            logs.AddRange(TransactionTableLogsMap);
            foreach (var pair in transactionState.UncommittedTransactionLog.TransactionTableLogMap)
            {
                var tableName = pair.Key;
                var txTableLog = pair.Value;
                var inMemoryBlocks = ImmutableArray<IBlock>.Empty.ToBuilder();

                if (logs.ContainsKey(tableName))
                {
                    if (txTableLog.CommittedDataBlock == null)
                    {
                        inMemoryBlocks.AddRange(logs[tableName].InMemoryBlocks);
                    }
                    else
                    {   //  Replace committed
                        var inTransactionCommittedCount = transactionState
                            .InMemoryDatabase
                            .TransactionTableLogsMap[tableName]
                            .InMemoryBlocks
                            .Count;

                        inMemoryBlocks.AddRange(logs[tableName].InMemoryBlocks
                            .Skip(inTransactionCommittedCount));
                        if (((IBlock)txTableLog.CommittedDataBlock).RecordCount != 0)
                        {
                            inMemoryBlocks.Add(txTableLog.CommittedDataBlock);
                        }
                    }
                }

                //  New data
                if (((IBlock)txTableLog.NewDataBlock).RecordCount != 0)
                {
                    inMemoryBlocks.Add(txTableLog.NewDataBlock);
                }
                if (inMemoryBlocks.Count > 0)
                {
                    logs[tableName] =
                        new ImmutableTableTransactionLogs(inMemoryBlocks.ToImmutable());
                }
                else if (logs.ContainsKey(tableName))
                {   //  The transaction emptied the blocks
                    logs.Remove(tableName);
                }
            }

            return new InMemoryDatabase(logs.ToImmutable());
        }
    }
}
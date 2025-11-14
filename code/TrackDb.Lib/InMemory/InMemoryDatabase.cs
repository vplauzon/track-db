using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory
{
    internal record InMemoryDatabase(
        IImmutableDictionary<string, ImmutableTableTransactionLogs> TableTransactionLogsMap)
    {
        public InMemoryDatabase()
            : this(ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty)
        {
        }

        public InMemoryDatabase CommitLog(TransactionState transactionState)
        {
            var logs = ImmutableDictionary<string, ImmutableTableTransactionLogs>
                .Empty
                .ToBuilder();

            logs.AddRange(TableTransactionLogsMap);
            foreach (var pair in transactionState.UncommittedTransactionLog.TransactionTableLogMap)
            {
                var tableName = pair.Key;
                var txTableLog = pair.Value;
                var inMemoryBlocks = ImmutableArray<IBlock>.Empty.ToBuilder();

                if (logs.ContainsKey(tableName))
                {
                    inMemoryBlocks.AddRange(logs[tableName].InMemoryBlocks);
                }

                //  Committed data
                if (txTableLog.CommittedDataBlock != null)
                {   //  Replace committed
                    var existingCommittedCount = transactionState
                        .InMemoryDatabase
                        .TableTransactionLogsMap[tableName]
                        .InMemoryBlocks
                        .Count;

                    //  We remove the previous committed to replace them with merged one
                    inMemoryBlocks.RemoveRange(0, existingCommittedCount);
                    if (txTableLog.CommittedDataBlock.RecordCount != 0)
                    {
                        inMemoryBlocks.Add(txTableLog.CommittedDataBlock);
                    }
                }
                //  New data
                if (((IBlock)txTableLog.NewDataBlockBuilder).RecordCount != 0)
                {
                    inMemoryBlocks.Add(txTableLog.NewDataBlockBuilder);
                }
                logs[tableName] =
                    new ImmutableTableTransactionLogs(inMemoryBlocks.ToImmutable());
            }

            return new InMemoryDatabase(logs.ToImmutable());
        }
    }
}
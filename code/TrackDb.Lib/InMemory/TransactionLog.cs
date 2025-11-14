using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory
{
    internal record TransactionLog(
        IDictionary<string, TransactionTableLog> TransactionTableLogMap)
    {
        public TransactionLog()
            : this(new Dictionary<string, TransactionTableLog>())
        {
        }

        public bool IsEmpty => TransactionTableLogMap.Values
            .Select(t => ((IBlock)t.NewDataBlockBuilder).RecordCount == 0
            && (t.CommittedDataBlock == null || ((IBlock)t.CommittedDataBlock).RecordCount == 0))
            .All(b => b);

        public void AppendRecord(
            DateTime creationTime,
            long recordId,
            ReadOnlySpan<object?> record,
            TableSchema schema)
        {
            EnsureTable(schema);
            TransactionTableLogMap[schema.TableName]
                .NewDataBlockBuilder
                .AppendRecord(creationTime, recordId, record);
        }

        public void AppendBlock(IBlock block)
        {
            EnsureTable(block.TableSchema);
            TransactionTableLogMap[block.TableSchema.TableName]
                .NewDataBlockBuilder
                .AppendBlock(block);
        }

        private void EnsureTable(TableSchema schema)
        {
            if (!TransactionTableLogMap.ContainsKey(schema.TableName))
            {
                TransactionTableLogMap.Add(schema.TableName, new(schema));
            }
        }
    }
}
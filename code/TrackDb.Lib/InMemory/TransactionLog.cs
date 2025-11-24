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
            .All(t => ((IBlock)t.NewDataBlock).RecordCount == 0 && t.CommittedDataBlock == null);

        public void AppendRecord(
            DateTime creationTime,
            long recordId,
            ReadOnlySpan<object?> record,
            TableSchema schema)
        {
            EnsureTable(schema);
            TransactionTableLogMap[schema.TableName]
                .NewDataBlock
                .AppendRecord(creationTime, recordId, record);
        }

        public void AppendBlock(IBlock block)
        {
            EnsureTable(block.TableSchema);
            TransactionTableLogMap[block.TableSchema.TableName]
                .NewDataBlock
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
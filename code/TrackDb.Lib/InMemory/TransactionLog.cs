using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.SystemData;

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

        public (long AppendRecordCount, long TombstoneRecordCount) GetLoggedRecordCounts(
            IEnumerable<string> loggingTables,
            string tombstoneTableName)
        {
            var appendRecordCount = loggingTables
                .Where(t => TransactionTableLogMap.ContainsKey(t))
                .Sum(t => ((IBlock)TransactionTableLogMap[t].NewDataBlock).RecordCount);

            if (TransactionTableLogMap.TryGetValue(tombstoneTableName, out var tombstoneTableLog))
            {
                var tombstoneBlock = (IBlock)tombstoneTableLog.NewDataBlock;
                var tombstoneSchema = (TypedTableSchema<TombstoneRecord>)tombstoneBlock.TableSchema;
                var tombstoneRecordCount = tombstoneBlock.Project(
                    new object?[1],
                    tombstoneSchema.GetColumnIndexSubset(t => t.TableName),
                    Enumerable.Range(0, tombstoneBlock.RecordCount),
                    42)
                    .Select(mem => (string)mem.Span[0]!)
                    .Where(t => loggingTables.Contains(t))
                    .Count();

                return (appendRecordCount, tombstoneRecordCount);
            }
            else
            {
                return (appendRecordCount, 0);
            }
        }

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

        internal void AppendCommittedRecord(
            DateTime creationTime,
            long recordId,
            ReadOnlySpan<object?> record,
            TableSchema schema)
        {
            TransactionTableLogMap[schema.TableName]
                .CommittedDataBlock!
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
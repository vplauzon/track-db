using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.InMemory
{
    internal class TransactionLog
    {
        public TransactionLog()
        {
            TransactionTableLogMap = new Dictionary<string, TransactionTableLog>();
        }

        public IDictionary<string, TransactionTableLog> TransactionTableLogMap { get; }

        public Dictionary<int, BlockTombstones>? ReplacingBlockTombstonesIndex { get; set; }

        public Dictionary<BlockAvailability, Dictionary<int, AvailableBlock>>?
            ReplacingAvailableBlockIndex { get; set; }

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
                    Enumerable.Range(0, tombstoneBlock.RecordCount))
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

        public void AppendRecord(ReadOnlySpan<object?> record, TableSchema schema)
        {
            EnsureTable(schema);
            TransactionTableLogMap[schema.TableName]
                .NewDataBlock
                .AppendRecord(record);
        }

        public void UpdateLastRecordIdMap(IDictionary<string, long> tableToLastRecordIdMap)
        {
            foreach (var p in TransactionTableLogMap)
            {
                var tableName = p.Key;
                var tableLog = p.Value;
                IBlock block = tableLog.NewDataBlock;
                var schema = (DataTableSchema)block.TableSchema;

                if (block.RecordCount > 0)
                {
                    var maxRecordId = block.Project(
                        new object?[1],
                        [schema.RecordIdColumnIndex],
                        Enumerable.Range(0, block.RecordCount))
                        .Max(r => (long)r.Span[0]!);

                    if (tableToLastRecordIdMap.ContainsKey(tableName))
                    {
                        if (tableToLastRecordIdMap[tableName] < maxRecordId)
                        {
                            tableToLastRecordIdMap[tableName] = maxRecordId;
                        }
                    }
                    else
                    {
                        tableToLastRecordIdMap[tableName] = maxRecordId;
                    }
                }
            }
        }

        public void EnsureTable(TableSchema schema)
        {
            if (!TransactionTableLogMap.ContainsKey(schema.TableName))
            {
                TransactionTableLogMap.Add(schema.TableName, new(schema));
            }
        }
    }
}
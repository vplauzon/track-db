using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Logging
{
    internal record TransactionContent(
        IImmutableDictionary<string, TableTransactionContent> Tables)
        : ContentBase<TransactionContent>
    {
        public int GetRowCount()
        {
            return Tables.Values
                .Sum(t => t.GetRowCount());
        }

        public static TransactionContent FromTransactionLog(
            TransactionLog transactionLog,
            TypedTableSchema<TombstoneRecord> tombstoneSchema,
            IImmutableDictionary<string, TableSchema> tableSchemaMap)
        {
            IImmutableList<long>? GetTombstoneRecordIds(IBlock? tombstoneBlock, string tableName)
            {
                if (tombstoneBlock == null)
                {
                    return null;
                }
                else
                {
                    var pf = new QueryPredicateFactory<TombstoneRecord>(tombstoneSchema);
                    var tableNamePredicate = pf
                        .Equal(t => t.TableName, tableName)
                        .QueryPredicate;
                    var rowIndexes = tombstoneBlock.Filter(tableNamePredicate, false).RowIndexes;
                    var columnIndexes = tombstoneSchema.GetColumnIndexSubset(t => t.DeletedRecordId);
                    var buffer = new object[1];
                    var ids = tombstoneBlock
                        .Project(buffer, columnIndexes.ToImmutableArray(), rowIndexes)
                        .Select(data => (long)data.Span[0]!)
                        .ToImmutableArray();

                    return ids.Length > 0 ? ids : null;
                }
            }

            var contentMapBuilder = ImmutableDictionary<string, TableTransactionContent>.Empty.ToBuilder();
            var transactionTableLogMap = transactionLog.TransactionTableLogMap;
            var tombstoneBlock = transactionTableLogMap.ContainsKey(tombstoneSchema.TableName)
                ? transactionTableLogMap[tombstoneSchema.TableName].NewDataBlock
                : null;

            foreach (var tableName in tableSchemaMap.Keys)
            {
                var newRecordsContent = transactionTableLogMap.ContainsKey(tableName)
                    && ((IBlock)transactionTableLogMap[tableName].NewDataBlock).RecordCount > 0
                    ? ToLog(transactionTableLogMap[tableName].NewDataBlock)
                    : null;
                var tombstoneRecordIds = GetTombstoneRecordIds(tombstoneBlock, tableName);

                if (newRecordsContent != null || tombstoneRecordIds != null)
                {
                    contentMapBuilder.Add(tableName, new(newRecordsContent, tombstoneRecordIds));
                }
            }

            return new(contentMapBuilder.ToImmutable());
        }

        public static TransactionLog ToTransactionLog(
            IReadOnlyList<TransactionContent> transactionContents,
            TypedTable<TombstoneRecord> tombstoneTable,
            IImmutableDictionary<string, TableSchema> tableSchemaMap)
        {
            var transactionLog = new TransactionLog();
            var groupByTableName = transactionContents
                .SelectMany(c => c.Tables)
                .GroupBy(p => p.Key, p => p.Value);

            foreach (var group in groupByTableName)
            {
                var tableName = group.Key;
                var tableTransactionContents = group;
                var newRecordsContents = group
                    .Where(ttc => ttc.NewRecordsContent != null)
                    .Select(ttc => ttc.NewRecordsContent!);
                var tombstoneRecordIds = group
                    .Where(ttc => ttc.TombstoneRecordIds != null)
                    .SelectMany(ttc => ttc.TombstoneRecordIds!)
                    .ToArray();

                //	NewRecordsContent
                if (newRecordsContents.Any())
                {
                    var tableSchema = tableSchemaMap[tableName];
                    var buffer = new object[tableSchema.Columns.Count];
                    var rowCount = newRecordsContents
                        .Sum(c => c.NewRecordIds.Count);
                    var blockBuilder = new BlockBuilder(tableSchema, rowCount);

                    AppendLog(blockBuilder, newRecordsContents);
                    transactionLog.TransactionTableLogMap.Add(
                        tableName,
                        new TransactionTableLog(blockBuilder));
                }
                //  Tombstone records
                if (tombstoneRecordIds.Length > 0)
                {
                    var tombstoneRecords = tombstoneRecordIds
                        .Select(id => new TombstoneRecord(id, tableName));
                    var recordWithRecordId =
                        new object?[tombstoneTable.Schema.ColumnProperties.Count];
                    var newRecordIds = tombstoneTable.NewRecordIds(tombstoneRecordIds.Length);
                    var zipped = tombstoneRecords.Zip(newRecordIds);

                    foreach (var z in zipped)
                    {
                        var tombstoneRecord = z.First;
                        var recordId = z.Second;
                        var recordWithoutRecordId =
                            tombstoneTable.Schema.FromObjectToColumns(tombstoneRecord);

                        recordWithoutRecordId.CopyTo(
                            recordWithRecordId.AsSpan().Slice(0, recordWithoutRecordId.Length));
                        recordWithRecordId[recordWithRecordId.Length - 1] = recordId;
                        transactionLog.AppendRecord(recordWithRecordId, tombstoneTable.Schema);
                    }
                }
            }

            return transactionLog;
        }

        public override string ToJson()
        {
            if (Tables.Count == 0)
            {
                return string.Empty;
            }
            else
            {
                return base.ToJson();
            }
        }

        private static NewRecordsContent ToLog(BlockBuilder blockBuilder)
        {
            IBlock block = blockBuilder;
            var schema = (DataTableSchema)block.TableSchema;
            var newRecordIds = block.Project(
                new object?[1],
                [schema.RecordIdColumnIndex],
                Enumerable.Range(0, block.RecordCount))
                .Select(r => (long)r.Span[0]!)
                .ToImmutableArray();
            var columnContentMap = schema.Columns
                .Index()
                .Select(p => KeyValuePair.Create(
                    p.Item.ColumnName,
                    blockBuilder.GetLogValues(p.Index).ToList()))
                .ToImmutableDictionary();

            return new NewRecordsContent(newRecordIds, columnContentMap);
        }

        private static void AppendLog(
            BlockBuilder blockBuilder,
            IEnumerable<NewRecordsContent> newRecordsContents)
        {
            IBlock block = blockBuilder;
            var schema = (DataTableSchema)block.TableSchema;
            var recordIds = newRecordsContents
                .SelectMany(c => c.NewRecordIds)
                .Select(i => JsonSerializer.SerializeToElement(i))
                .ToArray();
            var dataColumns = schema.Columns
                .Select(c => newRecordsContents.SelectMany(nrc => nrc.Columns[c.ColumnName]).ToArray())
                .Cast<IReadOnlyList<JsonElement>>()
                .Append(recordIds);

            blockBuilder.AppendLogs(dataColumns);
        }
    }
}
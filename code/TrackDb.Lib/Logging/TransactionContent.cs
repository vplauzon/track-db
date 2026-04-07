using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace TrackDb.Lib.Logging
{
    internal record TransactionContent(
        IImmutableDictionary<string, TableTransactionContent> Tables)
        : ContentBase<TransactionContent>
    {
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

        public TransactionLog ToTransactionLog(
            TypedTable<TombstoneRecord> tombstoneTable,
            IImmutableDictionary<string, TableSchema> tableSchemaMap)
        {
            var transactionLog = new TransactionLog();

            foreach (var pair in Tables)
            {
                var tableName = pair.Key;
                var tableTransactionContent = pair.Value;

                //	NewRecordsContent
                if (tableTransactionContent.NewRecordsContent != null)
                {
                    var tableSchema = tableSchemaMap[tableName];
                    var buffer = new object[tableSchema.Columns.Count];
                    var blockBuilder = new BlockBuilder(tableSchema);

                    AppendLog(blockBuilder, tableTransactionContent.NewRecordsContent);
                    transactionLog.TransactionTableLogMap.Add(tableName, new(blockBuilder));
                }
                //  Tombstone records
                if (tableTransactionContent.TombstoneRecordIds != null
                    && tableTransactionContent.TombstoneRecordIds.Count > 0)
                {
                    var tombstoneRecords = tableTransactionContent.TombstoneRecordIds
                        .Select(id => new TombstoneRecord(id, tableName, DateTime.Now));
                    var recordWithRecordId =
                        new object?[tombstoneTable.Schema.ColumnProperties.Count];
                    var newRecordIds = tombstoneTable.NewRecordIds(
                        tableTransactionContent.TombstoneRecordIds.Count);
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
            NewRecordsContent newRecordsContent)
        {
            IBlock block = blockBuilder;
            var schema = (DataTableSchema)block.TableSchema;
            var recordIds = newRecordsContent.NewRecordIds
                .Select(i => JsonSerializer.SerializeToElement(i))
                .ToArray();
            var dataColumns = schema.Columns
                .Select(c => newRecordsContent.Columns[c.ColumnName])
                .Cast<IReadOnlyList<JsonElement>>()
                .Append(recordIds);

            blockBuilder.AppendLogs(dataColumns);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
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
                        .Project(buffer, columnIndexes.ToImmutableArray(), rowIndexes, 0)
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
                    ? transactionTableLogMap[tableName].NewDataBlock.ToLog()
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

                    blockBuilder.AppendLog(tableTransactionContent.NewRecordsContent);
                    transactionLog.TransactionTableLogMap.Add(tableName, new(blockBuilder));
                }
                //  Tombstones
                if (tableTransactionContent.TombstoneRecordIds != null)
                {
                    var recordIds = pair.Value;
                    var tombstoneRecords = tableTransactionContent.TombstoneRecordIds
                        .Select(id => new TombstoneRecord(id, tableName, DateTime.Now));

                    foreach (var tombstoneRecord in tombstoneRecords)
                    {
                        transactionLog.AppendRecord(
                            DateTime.Now,
                            tombstoneTable.NewRecordId(),
                            tombstoneTable.Schema.FromObjectToColumns(tombstoneRecord),
                            tombstoneTable.Schema);
                    }
                }
            }

            return transactionLog;
        }
    }
}
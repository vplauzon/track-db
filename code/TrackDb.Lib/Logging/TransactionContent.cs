using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
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
        IImmutableDictionary<string, TableTransactionContent> Tables,
        IImmutableDictionary<string, TombstoneContent> Tombstones)
        : ContentBase<TransactionContent>
    {
        public static TransactionContent? FromTransactionLog(
            TransactionLog transactionLog,
            TypedTableSchema<TombstoneRecord> tombstoneSchema,
            IImmutableDictionary<string, TableSchema> tableSchemaMap)
        {
            IImmutableDictionary<string, TombstoneContent> ToTombstones(IBlock block)
            {
                var pf = new QueryPredicateFactory<TombstoneRecord>(tombstoneSchema);
                var isUserTablePredicate = pf
                    .In(t => t.TableName, tableSchemaMap.Keys)
                    .QueryPredicate;
                var rowIndexes = block.Filter(isUserTablePredicate, false).RowIndexes;
                var columnIndexes = tombstoneSchema.GetColumnIndexSubset(t => t.TableName)
                    .Concat(tombstoneSchema.GetColumnIndexSubset(t => t.DeletedRecordId))
                    //  Add the record-id of the tombstone record itself
                    .Append(tombstoneSchema.Columns.Count);
                var buffer = new object[3];
                var content = block.Project(buffer, columnIndexes, rowIndexes, 0)
                    .Select(data => new
                    {
                        Table = (string)data.Span[0]!,
                        DeletedRecordId = (long)data.Span[1]!,
                        RecordId = (long)data.Span[2]!
                    })
                    .GroupBy(o => o.Table)
                    .ToImmutableDictionary(
                    g => g.Key,
                    g => new TombstoneContent(
                        g.Select(i => i.RecordId).ToImmutableArray(),
                        g.Select(i => i.DeletedRecordId).ToImmutableArray()));

                return content;
            }

            var userTables = transactionLog.TableBlockBuilderMap
                .Where(p => tableSchemaMap.ContainsKey(p.Key))
                .Select(p => KeyValuePair.Create(p.Key, p.Value.ToLog()))
                .ToImmutableDictionary();
            var tombstoneRecordMap = transactionLog.TableBlockBuilderMap.ContainsKey(
                tombstoneSchema.TableName)
                ? ToTombstones(
                    transactionLog.TableBlockBuilderMap[tombstoneSchema.TableName])
                : ImmutableDictionary<string, TombstoneContent>.Empty;

            if (userTables.Any() || tombstoneRecordMap.Any())
            {
                var content = new TransactionContent(userTables, tombstoneRecordMap);

                return content;
            }
            else
            {
                return null;
            }
        }

        public TransactionLog ToTransactionLog(
            TypedTableSchema<TombstoneRecord> tombstoneSchema,
            IImmutableDictionary<string, TableSchema> tableSchemaMap)
        {
            var transactionLog = new TransactionLog();

            //  Tombstones
            foreach (var tombstoneTable in Tombstones)
            {
                var tableName = tombstoneTable.Key;
                var tombstoneContent = tombstoneTable.Value;
                var records = tombstoneContent.RecordId.Zip(
                    tombstoneContent.DeletedRecordId,
                    (recordId, deletedRecordId) => new
                    {
                        RecordId = recordId,
                        DeletedRecordId = deletedRecordId
                    });

                foreach (var record in records)
                {
                    transactionLog.AppendRecord(
                        record.RecordId,
                        tombstoneSchema.FromObjectToColumns(
                            new(record.DeletedRecordId, null, tableName, DateTime.Now)),
                        tombstoneSchema);
                }
            }
            //  Tables
            foreach (var table in Tables)
            {
                var tableName = table.Key;
                var tableTransactionContent = table.Value;
                var tableSchema = tableSchemaMap[tableName];
                var buffer = new object[tableSchema.Columns.Count];
                var blockBuilder = new BlockBuilder(tableSchema);

                blockBuilder.AppendLog(tableTransactionContent);
                transactionLog.TableBlockBuilderMap.Add(tableName, blockBuilder);
            }

            return transactionLog;
        }
    }
}
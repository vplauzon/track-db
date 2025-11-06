using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Statistics
{
    public record DatabaseStatistics(
        int MaxTableGeneration,
        DataStatistics GlobalStatistics,
        IImmutableDictionary<string, DataStatistics> TableStatistics)
    {
        #region Constructor
        internal static DatabaseStatistics Create(
            DatabaseState state,
            TypedTable<TombstoneRecord> tombstoneTable,
            TransactionContext tx)
        {
            var inMemoryDatabase = tx.TransactionState.InMemoryDatabase;
            var tableTransactionLogsMap = inMemoryDatabase.TableTransactionLogsMap;
            var tombstoneTableName = tombstoneTable.Schema.TableName;
            var tableMap = state.TableMap;
            var inMemoryRecordCountMap = tableMap
                .Where(p => p.Key != tombstoneTableName)
                .Select(t => new
                {
                    TableName = t.Key,
                    RecordCount = tableTransactionLogsMap.ContainsKey(t.Key)
                    ? tableTransactionLogsMap[t.Key].InMemoryBlocks.Sum(block => block.RecordCount)
                    : 0,
                    TombstoneRecordCount = tombstoneTable.Query(tx)
                    .Where(ts => ts.TableName == t.Key)
                    .Count()
                })
                .ToImmutableDictionary(t => t.TableName);
            var onDiskMap = tableMap
                .Where(p => p.Value.MetaDataTableName != null)
                .Select(p => new
                {
                    p.Value.Table.Schema.TableName,
                    MetaDataTable = tableMap[p.Value.MetaDataTableName!].Table,
                    MetadataSchemaManager = MetadataSchemaManager.FromMetadataTableSchema(
                        tableMap[p.Value.MetaDataTableName!].Table.Schema),
                })
                .Select(o => new
                {
                    o.TableName,
                    BlockStats = o.MetaDataTable.Query(tx)
                    .WithProjection([
                        o.MetadataSchemaManager.ItemCountColumnIndex,
                        o.MetadataSchemaManager.SizeColumnIndex])
                    .Select(b => new
                    {
                        RecordCount = (long)(int)b.Span[0]!,
                        BlockCount = 1,
                        Size = (long)(int)b.Span[1]!
                    })
                    //  Summarize all blocks
                    .Aggregate((o1, o2) => new
                    {
                        RecordCount = o1.RecordCount + o2.RecordCount,
                        BlockCount = o1.BlockCount + o2.BlockCount,
                        Size = o1.Size + o2.Size
                    })
                })
                .ToImmutableDictionary(o => o.TableName, o => o.BlockStats);
            var tableStatistics = tableMap
                .Where(p => p.Key != tombstoneTableName)
                .Select(t => t.Key)
                .ToImmutableDictionary(
                t => t,
                tableName => new DataStatistics(
                    inMemoryRecordCountMap[tableName].RecordCount,
                    inMemoryRecordCountMap[tableName].TombstoneRecordCount,
                    onDiskMap.ContainsKey(tableName) ? onDiskMap[tableName].BlockCount : 0,
                    onDiskMap.ContainsKey(tableName) ? onDiskMap[tableName].RecordCount : 0,
                    onDiskMap.ContainsKey(tableName) ? onDiskMap[tableName].Size : 0));
            var globalStatistics = new DataStatistics(
                tableStatistics.Sum(t => t.Value.InMemoryTableRecords),
                tableStatistics.Sum(t => t.Value.InMemoryTombstoneRecords),
                tableStatistics.Sum(t => t.Value.OnDiskBlockCount),
                tableStatistics.Sum(t => t.Value.OnDiskRecordCount),
                tableStatistics.Sum(t => t.Value.OnDiskSize));
            var maxTableGeneration = GetMaxTableGeneration(tableMap);

            return new DatabaseStatistics(maxTableGeneration, globalStatistics, tableStatistics);
        }

        private static int GetMaxTableGeneration(
            IImmutableDictionary<string, TableProperties> tableMap)
        {
            int GetTableGeneration(
                string tableName,
                IImmutableDictionary<string, TableProperties> tableMap)
            {
                var properties = tableMap[tableName];

                return properties.MetaDataTableName != null
                    ? 1 + GetTableGeneration(properties.MetaDataTableName, tableMap)
                    : 1;
            }

            var maxTableGeneration = tableMap.Values
                .Where(t => !t.IsMetaDataTable && t.IsPersisted)
                .Select(t => GetTableGeneration(t.Table.Schema.TableName, tableMap))
                .Max();

            return maxTableGeneration;
        }
        #endregion
    }
}
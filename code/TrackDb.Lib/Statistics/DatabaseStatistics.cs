using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Statistics
{
    public record DatabaseStatistics(
        int MaxTableGeneration,
        int InMemoryTombstoneRecords,
        DataStatistics NonMetadataStatistics,
        DataStatistics MetadataStatistics)
    {
        #region Constructor
        internal static DatabaseStatistics Create(
            DatabaseState state,
            TypedTable<TombstoneRecord> tombstoneTable,
            TransactionContext tx)
        {
            var inMemoryDatabase = tx.TransactionState.InMemoryDatabase;
            var tableMap = state.TableMap;
            var inMemoryRecordCountMap = inMemoryDatabase.TableTransactionLogsMap
                .Select(p => new
                {
                    IsMetaData = tableMap[p.Key].IsMetaDataTable,
                    RecordCount = p.Value.InMemoryBlocks.Sum(block => block.RecordCount)
                })
                .GroupBy(o => o.IsMetaData)
                .ToImmutableDictionary(g => g.Key, g => g.Sum(o => o.RecordCount));
            var inMemoryTombstoneRecordCount = inMemoryDatabase.TableTransactionLogsMap
                .Where(p => p.Key == tombstoneTable.Schema.TableName)
                .SelectMany(p => p.Value.InMemoryBlocks)
                .Sum(b => b.RecordCount);
            var onDiskMap = tableMap
                .Where(p => p.Value.MetaDataTableName != null)
                .Select(p => new
                {
                    p.Value.IsMetaDataTable,
                    MetaDataTable = tableMap[p.Value.MetaDataTableName!].Table
                })
                .Select(o => new
                {
                    o.IsMetaDataTable,
                    o.MetaDataTable,
                    MetadataSchemaManager = MetadataSchemaManager.FromMetadataTableSchema(
                        o.MetaDataTable.Schema),
                })
                .Select(o => new
                {
                    o.IsMetaDataTable,
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
                    .Aggregate((o1, o2) => new
                    {
                        RecordCount = o1.RecordCount + o2.RecordCount,
                        BlockCount = o1.BlockCount + o2.BlockCount,
                        Size = o1.Size + o2.Size
                    })
                })
                .GroupBy(o => o.IsMetaDataTable)
                .ToImmutableDictionary(
                g => g.Key,
                g => g.Select(o => o.BlockStats).Aggregate((o1, o2) => new
                {
                    RecordCount = o1.RecordCount + o2.RecordCount,
                    BlockCount = o1.BlockCount + o2.BlockCount,
                    Size = o1.Size + o2.Size
                }));

            return new DatabaseStatistics(
                GetMaxTableGeneration(tableMap),
                inMemoryTombstoneRecordCount,
                new(
                    inMemoryRecordCountMap.ContainsKey(false) ? inMemoryRecordCountMap[false] : 0,
                    onDiskMap.ContainsKey(false) ? onDiskMap[false].BlockCount : 0,
                    onDiskMap.ContainsKey(false) ? onDiskMap[false].RecordCount : 0,
                    onDiskMap.ContainsKey(false) ? onDiskMap[false].Size : 0),
                new(
                    inMemoryRecordCountMap.ContainsKey(true) ? inMemoryRecordCountMap[true] : 0,
                    onDiskMap.ContainsKey(true) ? onDiskMap[true].BlockCount : 0,
                    onDiskMap.ContainsKey(true) ? onDiskMap[true].RecordCount : 0,
                    onDiskMap.ContainsKey(true) ? onDiskMap[true].Size : 0));
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
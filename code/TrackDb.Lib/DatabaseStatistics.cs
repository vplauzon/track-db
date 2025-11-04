using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    public record DatabaseStatistics(
        int InMemoryNonMetadataTableRecords,
        int InMemoryMetadataTableRecords,
        int InMemoryTombstoneRecords,
        int MaxTableGeneration,
        int OnDiskNonMetadataBlocks,
        int OnDiskMetadataBlocks)
    //,
    //long OnDiskNonMetadataRecords,
    //long OnDiskMetadataRecords)
    //long OnDiskNonMetadataBytes,
    //long OnDiskMetadataBytes)
    {
        #region Constructor
        internal static DatabaseStatistics Create(
            DatabaseState state,
            TypedTable<TombstoneRecord> tombstoneTable,
            TransactionContext tx)
        {
            var inMemoryDatabase = tx.TransactionState.InMemoryDatabase;
            var tableMap = state.TableMap;
            var recordCountMap = inMemoryDatabase.TableTransactionLogsMap
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
                    BlockCount = o.MetaDataTable.Query(tx).Count()
                })
                .GroupBy(o => o.IsMetaDataTable)
                .ToImmutableDictionary(g => g.Key, g => (int)g.Sum(o => o.BlockCount));

            return new DatabaseStatistics(
                recordCountMap[false],
                recordCountMap.ContainsKey(true) ? recordCountMap[true] : 0,
                inMemoryTombstoneRecordCount,
                GetMaxTableGeneration(tableMap),
                onDiskMap.ContainsKey(false) ? onDiskMap[false] : 0,
                onDiskMap.ContainsKey(true) ? onDiskMap[true] : 0);
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
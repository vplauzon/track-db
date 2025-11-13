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
            var persistedMap = tableMap
                .Where(p => p.Value.MetaDataTableName != null)
                .Select(p => new
                {
                    p.Value.Table.Schema.TableName,
                    MetaDataTable = tableMap[p.Value.MetaDataTableName!].Table
                })
                .Select(o => new
                {
                    o.TableName,
                    BlockStats = o.MetaDataTable.Query(tx)
                    .WithProjection([
                        ((MetadataTableSchema) o.MetaDataTable.Schema).ItemCountColumnIndex,
                        ((MetadataTableSchema) o.MetaDataTable.Schema).SizeColumnIndex])
                    .Select(b => new
                    {
                        RecordCount = (long)(int)b.Span[0]!,
                        BlockCount = 1,
                        Size = (long)(int)b.Span[1]!
                    })
                    //  Summarize all blocks
                    .Aggregate(
                        new
                        {
                            RecordCount = (long)0,
                            BlockCount = 0,
                            Size = (long)0
                        },
                        (o1, o2) => new
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
                    new(
                        inMemoryRecordCountMap[tableName].RecordCount,
                        inMemoryRecordCountMap[tableName].TombstoneRecordCount),
                    new(
                        persistedMap.ContainsKey(tableName) ? persistedMap[tableName].BlockCount : 0,
                        persistedMap.ContainsKey(tableName) ? persistedMap[tableName].RecordCount : 0,
                        persistedMap.ContainsKey(tableName) ? persistedMap[tableName].Size : 0)));
            var globalStatistics = new DataStatistics(
                new(
                    tableStatistics.Sum(t => t.Value.InMemory.TableRecords),
                    tableStatistics.Sum(t => t.Value.InMemory.TombstoneRecords)),
                new(
                    tableStatistics.Sum(t => t.Value.Persisted.BlockCount),
                    tableStatistics.Sum(t => t.Value.Persisted.RecordCount),
                    tableStatistics.Sum(t => t.Value.Persisted.Size)));
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
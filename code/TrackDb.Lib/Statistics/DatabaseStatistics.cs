using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Statistics
{
    public record DatabaseStatistics(
        int MaxTableGeneration,
        DataStatistics GlobalStatistics,
        IImmutableDictionary<string, DataStatistics> TableStatistics)
    {
        #region Constructor
        internal static DatabaseStatistics Create(Database database)
        {
            using (var tx = database.CreateTransaction())
            {
                var tableMap = database.GetDatabaseStateSnapshot().TableMap;
                var maxTableGeneration = GetMaxTableGeneration(tableMap);
                var tableStatistics = tableMap.Values
                    .Select(p => new
                    {
                        p.Table,
                        MetadataTable = p.MetaDataTableName != null
                        ? tableMap[p.MetaDataTableName].Table
                        : null
                    })
                    .Select(o => KeyValuePair.Create(
                        o.Table.Schema.TableName,
                        DataStatistics.Create(database, o.Table, o.MetadataTable, tx)))
                    .ToImmutableDictionary();
                var persistedTableStatistics = tableStatistics
                    .Where(t => t.Value.Persisted != null)
                    .Select(t => t.Value.Persisted!);
                var globalStatistics = new DataStatistics(
                    new(
                        tableStatistics.Sum(t => t.Value.InMemory.Records),
                        tableStatistics.Sum(t => t.Value.InMemory.Tombstones)),
                    new(
                        persistedTableStatistics.Sum(t => t.BlockCount),
                        persistedTableStatistics.Sum(t => t.RecordCount),
                        persistedTableStatistics.Sum(t => t.Size)));

                return new DatabaseStatistics(maxTableGeneration, globalStatistics, tableStatistics);
            }
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
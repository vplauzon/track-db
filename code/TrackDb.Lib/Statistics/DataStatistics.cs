using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record DataStatistics(
        InMemoryDataStatistics InMemory,
        PersistedDataStatistics? Persisted)
    {
        #region Constructor
        internal static DataStatistics Create(
            Database database,
            Table table,
            Table? metadataTable,
            TransactionContext tx)
        {
            return new DataStatistics(
                InMemoryDataStatistics.Create(database, table, tx),
                metadataTable != null
                ? PersistedDataStatistics.Create(metadataTable, tx)
                : null);
        }
        #endregion
    }
}
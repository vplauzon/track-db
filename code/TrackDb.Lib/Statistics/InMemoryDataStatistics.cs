using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record InMemoryDataStatistics(long Records, long Tombstones)
    {
        #region Constructor
        internal static InMemoryDataStatistics Create(
            Database database,
            Table table,
            TransactionContext tx)
        {
            var tableRecords = table.Query(tx)
                .WithInMemoryOnly()
                .Count();
            var tombstoneRecords = database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, table.Schema.TableName))
                .Count();

            return new InMemoryDataStatistics(tableRecords, tombstoneRecords);
        }
        #endregion
    }
}
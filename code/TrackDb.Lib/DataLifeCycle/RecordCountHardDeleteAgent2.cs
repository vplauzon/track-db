using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent2 : DataLifeCycleAgentBase
    {
        public RecordCountHardDeleteAgent2(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll)
                == DataManagementActivity.HardDeleteAll;

            using (var tx = Database.CreateTransaction())
            {
                if (doHardDeleteAll || IsHardDeleteRequired(tx))
                {
                }

                tx.Complete();
            }
        }

        private bool IsHardDeleteRequired(TransactionContext tx)
        {
            bool IsAboveThreshold() => Database.TombstoneTable.Query(tx).Count()
                > Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            if (!IsAboveThreshold())
            {
                return false;
            }
            else
            {
                var tombstoneSchema = Database.TombstoneTable.Schema;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var tableCounts = Database.TombstoneTable.Query(tx)
                    .TableQuery
                    .WithProjection(tombstoneSchema.GetColumnIndexSubset(t => t.TableName))
                    .Select(r => (string)r.Span[0]!)
                    .CountBy(name => name)
                    .Select(o => new
                    {
                        TableName = o.Key,
                        RowCount = o.Value
                    })
                    .Where(o => tableMap[o.TableName].IsPersisted)
                    .OrderBy(o => o.RowCount)
                    .Select(o => o.TableName)
                    .ToList();

                do
                {
                    var tableToCompact = tableCounts.Last();
                    var hasLoaded = tx.LoadCommittedBlocksInTransaction(tableToCompact);

                    tableCounts.RemoveAt(tableCounts.Count - 1);
                    if(!IsAboveThreshold())
                    {
                        return false;
                    }
                }
                while (tableCounts.Count > 0);

                return true;
            }
        }
    }
}
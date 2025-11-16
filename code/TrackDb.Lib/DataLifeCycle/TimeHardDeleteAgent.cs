using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TimeHardDeleteAgent : HardDeleteAgentBase
    {
        public TimeHardDeleteAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected override TableRecord? FindUnmergedRecordCandidate(bool doHardDeleteAll)
        {
            using (var tx = Database.CreateTransaction())
            {
                var maxTombstonePeriod = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var query = TombstoneTable.Query(tx);

                if (doHardDeleteAll)
                {
                    var systemTables = tableMap
                        .Where(p => p.Value.IsSystemTable)
                        .Select(p => p.Key);
                    //  Avoid infinite loop by having system tables hard delete on command
                    query = query
                        .Where(pf => pf.NotIn(t => t.TableName, systemTables));
                }

                var oldestTombstone = query
                    .OrderBy(t => t.Timestamp)
                    .Take(1)
                    .FirstOrDefault();

                return oldestTombstone != null
                    && (doHardDeleteAll || DateTime.Now - oldestTombstone.Timestamp > maxTombstonePeriod)
                    ? new(oldestTombstone.TableName, oldestTombstone.DeletedRecordId)
                    : null;
            }
        }
    }
}
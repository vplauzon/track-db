using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;
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

        protected override (string TableName, long RecordId)? FindRecordCandidate(
            bool doHardDeleteAll)
        {
            using (var tx = Database.CreateTransaction())
            {
                var maxTombstonePeriod = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
                var oldestTombstone = TombstoneTable.Query(tx)
                    .OrderBy(t => t.Timestamp)
                    .Take(1)
                    .FirstOrDefault();

                return oldestTombstone != null
                    && (doHardDeleteAll || DateTime.Now - oldestTombstone.Timestamp > maxTombstonePeriod)
                    ? (oldestTombstone.TableName, oldestTombstone.DeletedRecordId)
                    : null;
            }
        }
    }
}
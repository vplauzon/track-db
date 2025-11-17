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

        protected override TableCandidate? FindUnmergedRecordCandidate(
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            var maxTombstonePeriod = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var query = TombstoneTable.Query(tx);
            var oldestCandidate = query
                //  Avoid infinite loop by having system tables hard delete on command
                .Where(t => !doHardDeleteAll || !tableMap[t.TableName].IsSystemTable)
                .OrderBy(t => t.Timestamp)
                .Take(1)
                .Select(t => new TableCandidate(t.TableName, t.DeletedRecordId))
                .FirstOrDefault();

            return oldestCandidate;
        }
    }
}
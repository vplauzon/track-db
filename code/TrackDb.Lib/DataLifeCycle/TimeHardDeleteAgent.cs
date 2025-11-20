using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TimeHardDeleteAgent : HardDeleteAgentBase
    {
        public TimeHardDeleteAgent(Database database)
            : base(database)
        {
        }

        protected override TableCandidate? FindCandidate(
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            var maxTombstonePeriod = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var query = Database.TombstoneTable.Query(tx);
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
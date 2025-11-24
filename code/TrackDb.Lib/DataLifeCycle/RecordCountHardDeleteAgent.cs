using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : HardDeleteAgentBase
    {
        public RecordCountHardDeleteAgent(Database database)
            : base(database)
        {
        }

        protected override TableCandidate? FindCandidate(
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            var maxTombstonedRecords = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            if (doHardDeleteAll || Database.TombstoneTable.Query(tx).Count() > maxTombstonedRecords)
            {
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var candidate = Database.TombstoneTable.Query(tx)
                    .GroupBy(t => t.TableName)
                    //  Avoid infinite loop by having system tables hard delete on command
                    .Where(g => !doHardDeleteAll || !tableMap[g.Key].IsSystemTable)
                    .OrderByDescending(g => g.Count())
                    .Take(1)
                    .Select(g => new TableCandidate(
                        g.Key,
                        g
                        .OrderBy(t => t.Timestamp)
                        .First()
                        .DeletedRecordId))
                    .FirstOrDefault();

                return candidate;
            }
            else
            {
                return null;
            }
        }
    }
}
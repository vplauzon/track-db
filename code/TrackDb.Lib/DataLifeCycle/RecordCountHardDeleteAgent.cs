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
        public RecordCountHardDeleteAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected override TableRecord? FindUnmergedRecordCandidate(bool doHardDeleteAll)
        {
            using (var tx = Database.CreateDummyTransaction())
            {
                var maxTombstonedRecords = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

                if (doHardDeleteAll || TombstoneTable.Query(tx).Count() > maxTombstonedRecords)
                {
                    var tombstoneGroups = TombstoneTable.Query(tx)
                        .GroupBy(t => new { t.TableName, t.BlockId });
                    string? tableName = null;
                    long recordId = -1;
                    var maxRecordCount = 0;
                    var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

                    tombstoneGroups = doHardDeleteAll
                        //  Avoid infinite loop by having system tables hard delete on command
                        ? tombstoneGroups.Where(g => !tableMap[g.Key.TableName].IsSystemTable)
                        : tombstoneGroups;

                    foreach (var group in tombstoneGroups)
                    {
                        if (group.Count() > maxRecordCount)
                        {
                            maxRecordCount = group.Count();
                            tableName = group.Key.TableName;
                            recordId = group.First().DeletedRecordId;
                        }
                    }

                    return tableName != null
                        ? new(tableName, recordId)
                        : null;
                }
                else
                {
                    return null;
                }
            }
        }
    }
}
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

        protected override (string TableName, long RecordId)? FindRecordCandidate(
            bool doHardDeleteAll)
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
                        ? (tableName, recordId)
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
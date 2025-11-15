using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class NonMetaRecordPersistanceAgent : RecordPersistanceAgentBase
    {
        public NonMetaRecordPersistanceAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected override int MaxInMemoryDataRecords =>
            Database.DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecords;

        protected override IEnumerable<KeyValuePair<string, ImmutableTableTransactionLogs>> GetTableLogs(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var inMemoryDb = tx.TransactionState.InMemoryDatabase;
            var logs = inMemoryDb.TransactionTableLogsMap
                .Select(p => new
                {
                    Pair = p,
                    TableProperties = tableMap[p.Key]
                })
                .Where(o => !o.TableProperties.IsMetaDataTable)
                .Where(o => o.TableProperties.IsPersisted)
                .Select(o => o.Pair);

            return logs;
        }

        protected override bool DoPersistAll(DataManagementActivity forcedActivity)
        {
            var doPersistEverything =
                (forcedActivity & DataManagementActivity.PersistAllNonMetaData) != 0;

            return doPersistEverything;
        }
    }
}

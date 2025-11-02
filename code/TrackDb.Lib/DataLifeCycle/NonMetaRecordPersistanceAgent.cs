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

        protected override IEnumerable<KeyValuePair<string, ImmutableTableTransactionLogs>> GetTableLogs(
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var inMemoryDb = tx.TransactionState.InMemoryDatabase;
            var logs = inMemoryDb.TableTransactionLogsMap
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

        protected override bool IsPersistanceRequired(
            DataManagementActivity forcedDataManagementActivity,
            TransactionContext tx)
        {
            var doPersistEverything =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllNonMetaData) != 0;

            var tableLogs = GetTableLogs(tx);
            var totalRecords = tableLogs
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            if (totalRecords > 0)
            {
                if (doPersistEverything)
                {
                    return true;
                }
                else
                {
                    var tableCount = tableLogs.Count();
                    var threshold = Math.Max(
                        Database.DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecordsInMemory,
                        tableCount * Database.DatabasePolicy.InMemoryPolicy.MinNonMetaDataRecordsPerBlock);

                    return totalRecords > threshold;
                }
            }

            return false;
        }
    }
}

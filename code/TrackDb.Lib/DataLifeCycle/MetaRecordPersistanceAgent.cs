using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaRecordPersistanceAgent : RecordPersistanceAgentBase
    {
        public MetaRecordPersistanceAgent(Database database)
            : base(database)
        {
        }

        protected override int MaxInMemoryDataRecords =>
            Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords;

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
                .Where(o => o.TableProperties.IsMetaDataTable)
                .Where(o => o.TableProperties.IsPersisted);

            if (DoPersistAll(forcedActivity))
            {   //  We limit to only 1st level metadata tables
                var firstLevelMetadataTable = tableMap.Values
                    .Where(t => !t.IsMetaDataTable && t.IsPersisted)
                    .Where(t => t.MetaDataTableName != null)
                    .Select(t => t.MetaDataTableName!)
                    .ToImmutableHashSet();

                logs = logs
                    .Where(o => firstLevelMetadataTable.Contains(o.Pair.Key));
            }

            return logs
                .Select(o => o.Pair);
        }

        protected override bool DoPersistAll(DataManagementActivity forcedActivity)
        {
            var doPersistEverything =
                (forcedActivity & DataManagementActivity.PersistAllMetaDataFirstLevel) != 0;

            return doPersistEverything;
        }
    }
}

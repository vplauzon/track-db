using System;
using System.Linq;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaRecordMergeAgent : DataLifeCycleAgentBase
    {
        public MetaRecordMergeAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                if (IsPersistanceRequired(tx))
                {
                    tx.Complete();

                    throw new NotImplementedException();
                }
                else
                {
                    return true;
                }
            }
        }

        private bool IsPersistanceRequired(TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tableLogs = tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap
                .Where(p => tableMap[p.Key].IsMetaDataTable)
                .Select(p => p.Value);
            var totalRecords = tableLogs
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return totalRecords > Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords;
        }
    }
}
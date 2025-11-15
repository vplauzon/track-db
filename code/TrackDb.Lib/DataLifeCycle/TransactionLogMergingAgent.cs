using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TransactionLogMergingAgent : DataLifeCycleAgentBase
    {

        public TransactionLogMergingAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doMergeAll =
                (forcedDataManagementActivity & DataManagementActivity.MergeAllInMemoryLogs) != 0;
            var maxInMemoryBlocksPerTable = doMergeAll
                ? 1
                : Database.DatabasePolicy.InMemoryPolicy.MaxBlocksPerTable;

            using(var tx = Database.CreateTransaction(false))
            {
                var candidateTables = tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap
                    .Where(p => p.Value.InMemoryBlocks.Count > maxInMemoryBlocksPerTable)
                    .Select(p => p.Key);

                foreach (var tableName in candidateTables)
                {
                    tx.LoadCommittedBlocksInTransaction(tableName);
                }

                tx.Complete();
            }

            return true;
        }
    }
}
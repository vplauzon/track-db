using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class LogMergingAgent : DataLifeCycleAgentBase
    {

        public LogMergingAgent(
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

            MergeTransactionLogs(doMergeAll);

            return true;
        }

        private void MergeTransactionLogs(bool doMergeAll)
        {
            for (var candidateTable = FindMergeCandidate(doMergeAll);
                 candidateTable != null;
                 candidateTable = FindMergeCandidate(doMergeAll))
            {
                MergeTableTransactionLogs(candidateTable);
            }
        }

        private string? FindMergeCandidate(bool doMergeAll)
        {
            var maxInMemoryBlocksPerTable = doMergeAll
                ? 1
                : Database.DatabasePolicy.InMemoryPolicy.MaxBlocksPerTable;

            return Database.GetDatabaseStateSnapshot().InMemoryDatabase.TableTransactionLogsMap
                .Where(p => p.Value.InMemoryBlocks.Count > maxInMemoryBlocksPerTable)
                .Select(p => p.Key)
                .FirstOrDefault();
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class LogMergingAgent : DataLifeCycleAgentBase
    {

        public LogMergingAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doMergeAll =
                (forcedDataManagementActivity & DataManagementActivity.MergeAllInMemoryLogs) != 0;

            return MergeTransactionLogs(doMergeAll);
        }

        private bool MergeTransactionLogs(bool doMergeAll)
        {
            var maxInMemoryBlocksPerTable = doMergeAll
                ? 1
                : Database.DatabasePolicies.MaxUnpersistedBlocksPerTable;
            var candidateTableName = Database.GetDatabaseStateSnapshot().DatabaseCache.TableTransactionLogsMap
                .Where(p => p.Value.InMemoryBlocks.Count > maxInMemoryBlocksPerTable)
                .Select(p => p.Key)
                .FirstOrDefault();

            if (candidateTableName != null)
            {
                using (var tc = Database.CreateDummyTransaction())
                {
                    (var tableBlock, var tombstoneBlock) =
                        MergeTableTransactionLogs(candidateTableName, tc);
                    var mapBuilder = ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder();

                    mapBuilder.Add(candidateTableName, tableBlock);
                    if (tombstoneBlock != null)
                    {
                        mapBuilder.Add(TombstoneTable.Schema.TableName, tombstoneBlock);
                    }
                    CommitAlteredLogs(mapBuilder.ToImmutable(), tc);
                }

                return MergeTransactionLogs(doMergeAll);
            }
            else
            {
                return true;
            }
        }
    }
}
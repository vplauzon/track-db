using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class ReleaseBlockAgent : DataLifeCycleAgentBase
    {

        public ReleaseBlockAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var dbState = Database.GetDatabaseStateSnapshot();
            var discardedBlockIds = Database.GetDatabaseStateSnapshot().DiscardedBlockIds;

            if (!dbState.TransactionMap.Any() && discardedBlockIds.Any())
            {   //  We don't want any transaction in case one uses the discarded blocks
                foreach (var blockId in discardedBlockIds)
                {
                    StorageManager.ReleaseBlock(blockId);
                }
                Database.ChangeDatabaseState(state =>
                {
                    return state with
                    {
                        DiscardedBlockIds = state.DiscardedBlockIds
                        .Skip(discardedBlockIds.Count)
                        .ToImmutableArray()
                    };
                });
            }

            return true;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class ReleaseBlockAgent : DataLifeCycleAgentBase
    {

        public ReleaseBlockAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DbFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var dbState = Database.GetDatabaseStateSnapshot();
            var discardedBlockIds = Database.GetDatabaseStateSnapshot().DiscardedBlockIds;

            if (!dbState.TransactionMap.Any() && discardedBlockIds.Any())
            {
                Database.ReleaseBlockIds(discardedBlockIds);
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
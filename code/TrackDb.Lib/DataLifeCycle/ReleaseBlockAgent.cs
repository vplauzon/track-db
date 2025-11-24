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
        public ReleaseBlockAgent(Database database)
            : base(database)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var dbState = Database.GetDatabaseStateSnapshot();
            var discardedBlockIds = dbState.DiscardedBlockIds;

            if (discardedBlockIds.Any() && !dbState.TransactionMap.Any())
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
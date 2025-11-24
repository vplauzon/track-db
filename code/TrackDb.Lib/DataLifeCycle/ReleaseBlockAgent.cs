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
            //  No active transaction
            if (!Database.HasActiveTransaction)
            {
                using (var tx = Database.CreateTransaction())
                {
                    Database.ReleaseNoLongerInUsedBlocks(tx);

                    tx.Complete();
                }
            }

            return true;
        }
    }
}
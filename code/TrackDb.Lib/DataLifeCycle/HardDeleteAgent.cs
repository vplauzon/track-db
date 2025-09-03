using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DbStorage;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class HardDeleteAgent : DataLifeCycleAgentBase
    {

        public HardDeleteAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll) != 0;

            return HardDelete(doHardDeleteAll);
        }

        private bool HardDelete(bool doHardDeleteAll)
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                return true;
            }
        }

    }
}
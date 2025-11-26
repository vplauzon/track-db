using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class NonMetaRecordPersistanceAgent : RecordPersistanceAgentBase
    {
        public NonMetaRecordPersistanceAgent(Database database)
            : base(database)
        {
        }

        protected override int MaxInMemoryDataRecords =>
            Database.DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecords;

        protected override IEnumerable<Table> GetTables(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tables = tableMap.Values
                .Where(tp => !tp.IsMetaDataTable)
                .Where(tp => tp.IsPersisted)
                .Select(tp => tp.Table);

            return tables;
        }

        protected override bool DoPersistAll(DataManagementActivity forcedActivity)
        {
            var doPersistEverything =
                (forcedActivity & DataManagementActivity.PersistAllNonMetaData) != 0;

            return doPersistEverything;
        }
    }
}

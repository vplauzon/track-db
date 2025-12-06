using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class MetaTableProvider : LogicBase, ITableProvider
    {
        public MetaTableProvider(Database database)
            : base(database)
        {
        }

        long ITableProvider.MaxInMemoryDataRecords
            => Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords;

        bool ITableProvider.DoPersistAll(DataManagementActivity activity)
        {
            var doPersistEverything =
                (activity & DataManagementActivity.PersistAllMetaDataFirstLevel) != 0;

            return doPersistEverything;
        }

        IEnumerable<Table> ITableProvider.GetTables(TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tableProperties = tableMap.Values
                .Where(tp => tp.IsMetaDataTable)
                .Where(tp => tp.IsPersisted);

            return tableProperties
                .Select(tp => tp.Table);
        }
    }
}
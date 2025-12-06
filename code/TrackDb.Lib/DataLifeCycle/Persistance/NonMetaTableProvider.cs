using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class NonMetaTableProvider : LogicBase, ITableProvider
    {
        public NonMetaTableProvider(Database database)
            : base(database)
        {
        }

        IEnumerable<Table> ITableProvider.GetTables(TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tables = tableMap.Values
                .Where(tp => !tp.IsMetaDataTable)
                .Where(tp => tp.IsPersisted)
                .Select(tp => tp.Table);

            return tables;
        }
    }
}
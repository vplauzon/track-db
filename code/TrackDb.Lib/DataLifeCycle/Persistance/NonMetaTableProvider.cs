using System;
using System.Collections.Generic;
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
            throw new NotImplementedException();
        }
    }
}
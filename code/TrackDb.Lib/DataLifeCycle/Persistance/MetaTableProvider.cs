using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class MetaTableProvider : ITableProvider
    {
        IEnumerable<Table> ITableProvider.GetTables(TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
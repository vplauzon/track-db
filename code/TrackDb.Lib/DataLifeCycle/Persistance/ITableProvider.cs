using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal interface ITableProvider
    {
        IEnumerable<Table> GetTables(TransactionContext tx);
    }
}
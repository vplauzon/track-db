using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal interface ITableProvider
    {
        IEnumerable<Table> GetTables(TransactionContext tx);
    }
}
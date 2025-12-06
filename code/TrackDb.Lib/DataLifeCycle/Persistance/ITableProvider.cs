using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal interface ITableProvider
    {
        long MaxInMemoryDataRecords { get; }

        bool DoPersistAll(DataManagementActivity activity);

        IEnumerable<Table> GetTables(TransactionContext tx);
    }
}
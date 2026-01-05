using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib
{
    /// <summary>Action for a trigger.</summary>
    /// <param name="databaseContext"></param>
    /// <param name="tx"></param>
    public delegate void TableTriggerAction(
        DatabaseContextBase databaseContext,
        TransactionContext tx);
}
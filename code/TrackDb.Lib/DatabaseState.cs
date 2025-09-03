using TrackDb.Lib.Cache;
using TrackDb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    /// <summary>
    /// Whole state for a database, including all cached data in the "main branch"
    /// and each transaction.
    /// </summary>
    /// <param name="DatabaseCache"></param>
    /// <param name="TransactionMap"></param>
    /// <param name="TableMap"></param>
    internal record DatabaseState(
        DatabaseCache DatabaseCache,
        IImmutableDictionary<long, TransactionState> TransactionMap,
        IImmutableDictionary<string, TableProperties> TableMap)
    {
        public DatabaseState(IImmutableDictionary<string, TableProperties> tableMap)
            : this(
                  new DatabaseCache(),
                  ImmutableDictionary<long, TransactionState>.Empty,
                  tableMap)
        {
        }
    }
}
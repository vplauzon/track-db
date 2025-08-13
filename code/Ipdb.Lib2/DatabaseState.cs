using Ipdb.Lib2.Cache;
using Ipdb.Lib2.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2
{
    /// <summary>
    /// Whole state for a database, including all cached data in the "main branch"
    /// and each transaction.
    /// </summary>
    /// <param name="DatabaseCache"></param>
    /// <param name="TransactionMap"></param>
    internal record DatabaseState(
        DatabaseCache DatabaseCache,
        IImmutableDictionary<long, TransactionState> TransactionMap)
    {
        public DatabaseState()
            : this(
                  new DatabaseCache(),
                  ImmutableDictionary<long, TransactionState>.Empty)
        {
        }
    }
}
using Ipdb.Lib.Cache;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    /// <summary>
    /// Whole state for a database, including all cached data in the "main branch"
    /// and each transaction.
    /// </summary>
    /// <param name="DatabaseCache"></param>
    /// <param name="TransactionMap"></param>
    internal record DatabaseState(
        DatabaseCache DatabaseCache,
        IImmutableDictionary<long, TransactionCache> TransactionMap)
    {
        public DatabaseState()
            : this(
                  new DatabaseCache(
                      ImmutableArray<ImmutableTransactionLog>.Empty,
                      new DocumentBlockCollection(),
                      new IndexBlockCollection()),
                  ImmutableDictionary<long, TransactionCache>.Empty)
        {
        }
    }
}
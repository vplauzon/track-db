using Ipdb.Lib2.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache
{
    internal record DatabaseCache(
        StorageBlockMap StorageBlockMap,
        IImmutableList<ImmutableTransactionLog> TransactionLogs)
    {
    }
}
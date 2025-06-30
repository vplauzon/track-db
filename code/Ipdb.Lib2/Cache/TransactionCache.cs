using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache
{
    internal record TransactionCache(
        DatabaseCache DatabaseCache,
        TransactionLog TransactionLog);
}
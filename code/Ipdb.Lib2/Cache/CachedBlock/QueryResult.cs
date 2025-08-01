using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record QueryResult(
        long RecordId,
        Func<object?[]> ProjectionFunc);
}
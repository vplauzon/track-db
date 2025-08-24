using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public interface ITypedQueryPredicate<T> : IQueryPredicate
        where T : notnull
    {
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public abstract class QueryPredicate<T>
    {
        internal abstract IImmutableList<Expression> GetProperties();

        internal abstract IImmutableList<short> CombineHash(
            IImmutableDictionary<Expression, IImmutableList<short>> hashMap);

        internal abstract IImmutableList<T> FilterDocuments(IImmutableList<T> documents);
    }
}

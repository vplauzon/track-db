using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal record TypedQueryPredicateAdapter<T>(
        IQueryPredicate Adaptee)
        : ITypedQueryPredicate<T>
        where T : notnull
    {
        bool IQueryPredicate.IsTerminal => Adaptee.IsTerminal;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => Adaptee.FirstPrimitivePredicate;

        IQueryPredicate? IQueryPredicate.Simplify(Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            return replaceFunc(Adaptee);
        }
    }
}
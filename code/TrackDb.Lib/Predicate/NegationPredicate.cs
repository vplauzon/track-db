using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal record NegationPredicate(IQueryPredicate InnerPredicate)
        : IQueryPredicate
    {
        bool IQueryPredicate.IsTerminal => false;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate =>
            InnerPredicate.FirstPrimitivePredicate;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            throw new NotImplementedException();
        }
    }
}
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
        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is NegationPredicate np
                && np.InnerPredicate.Equals(InnerPredicate);
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates => InnerPredicate.LeafPredicates;


        IQueryPredicate? IQueryPredicate.Simplify()
        {
            IQueryPredicate sp = new SubstractPredicate(
                AllInPredicate.Instance,
                InnerPredicate.Simplify() ?? InnerPredicate);

            return sp.Simplify() ?? sp;
        }

        IQueryPredicate? IQueryPredicate.Substitute(
            IQueryPredicate beforePredicate,
            IQueryPredicate afterPredicate)
        {
            if (beforePredicate.Equals(this))
            {
                return afterPredicate;
            }
            else
            {
                var si = InnerPredicate.Substitute(beforePredicate, afterPredicate);

                return si != null
                    ? new NegationPredicate(si)
                    : null;
            }
        }
    }
}
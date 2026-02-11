using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public sealed record NegationPredicate(QueryPredicate InnerPredicate)
        : QueryPredicate
    {
        internal override IEnumerable<int> ReferencedColumnIndexes => InnerPredicate.ReferencedColumnIndexes;

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => InnerPredicate.LeafPredicates;

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is NegationPredicate np
                && np.InnerPredicate.Equals(InnerPredicate);
        }

        internal override QueryPredicate? Simplify()
        {
            QueryPredicate sp = new SubstractPredicate(
                AllInPredicate.Instance,
                InnerPredicate.Simplify() ?? InnerPredicate);

            return sp.Simplify() ?? sp;
        }

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
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

        public override string ToString()
        {
            return $"!({InnerPredicate})";
        }
    }
}
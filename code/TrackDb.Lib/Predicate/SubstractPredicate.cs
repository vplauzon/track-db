using System;
using System.Collections.Generic;
using System.Linq;

namespace TrackDb.Lib.Predicate
{
    internal sealed record SubstractPredicate(
        QueryPredicate LeftPredicate,
        QueryPredicate RightPredicate) : QueryPredicate
    {
        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is SubstractPredicate sp
                && sp.LeftPredicate.Equals(LeftPredicate)
                && sp.RightPredicate.Equals(RightPredicate);
        }

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => LeftPredicate.LeafPredicates.Concat(RightPredicate.LeafPredicates);

        internal override QueryPredicate? Simplify()
        {
            if (LeftPredicate is ResultPredicate rpl && RightPredicate is ResultPredicate rpr)
            {
                return new ResultPredicate(rpl.RecordIndexes.Except(rpr.RecordIndexes));
            }
            else
            {
                var sl = LeftPredicate.Simplify();
                var sr = RightPredicate.Simplify();

                if (sl != null || sr != null)
                {
                    var simplified =
                        new SubstractPredicate(sl ?? LeftPredicate, sr ?? RightPredicate);

                    return simplified.Simplify() ?? simplified;
                }
                else
                {
                    return null;
                }
            }
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
                var sl = LeftPredicate.Substitute(beforePredicate, afterPredicate);
                var sr = RightPredicate.Substitute(beforePredicate, afterPredicate);

                return sl != null || sr != null
                    ? new SubstractPredicate(sl ?? LeftPredicate, sr ?? RightPredicate)
                    : null;
            }
        }

        public override string ToString()
        {
            return $"({LeftPredicate}) \\ ({RightPredicate})";
        }
    }
}
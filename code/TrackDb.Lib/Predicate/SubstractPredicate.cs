using System;
using System.Collections.Generic;
using System.Linq;

namespace TrackDb.Lib.Predicate
{
    internal record SubstractPredicate(
        IQueryPredicate LeftPredicate,
        IQueryPredicate RightPredicate) : IQueryPredicate
    {
        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is SubstractPredicate sp
                && sp.LeftPredicate.Equals(LeftPredicate)
                && sp.RightPredicate.Equals(RightPredicate);
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates =>
            LeftPredicate.LeafPredicates.Concat(RightPredicate.LeafPredicates);

        IQueryPredicate? IQueryPredicate.Simplify()
        {
            if (LeftPredicate is ResultPredicate rpl && RightPredicate is ResultPredicate rpr)
            {
                return new ResultPredicate(rpl.RecordIndexes.Except(rpr.RecordIndexes));
            }
            else
            {
                return null;
            }
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
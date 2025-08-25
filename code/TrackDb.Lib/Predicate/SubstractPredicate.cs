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
            if (RightPredicate is ResultPredicate rpr
                && LeftPredicate is ResultPredicate rpl)
            {
                return new ResultPredicate(rpl.RecordIndexes.Except(rpr.RecordIndexes));
            }
            else
            {
                return Recompose(LeftPredicate.Simplify(), RightPredicate.Simplify());
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
                return Recompose(LeftPredicate.Simplify(), RightPredicate.Simplify());
            }
        }

        private IQueryPredicate? Recompose(IQueryPredicate? sl, IQueryPredicate? sr)
        {
            if (sl != null && sr != null)
            {
                IQueryPredicate simplified =
                    new SubstractPredicate(sl ?? LeftPredicate, sr ?? RightPredicate);

                return simplified.Simplify() ?? simplified;
            }
            else
            {
                return null;
            }
        }
    }
}
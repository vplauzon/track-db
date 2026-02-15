using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    /// <summary>
    /// Applying an "AND" logic between two predicates
    /// </summary>
    /// <param name="LeftPredicate"></param>
    /// <param name="RightPredicate"></param>
    public sealed record ConjunctionPredicate(
        QueryPredicate LeftPredicate,
        QueryPredicate RightPredicate)
        : QueryPredicate
    {
        internal override IEnumerable<int> ReferencedColumnIndexes =>
            LeftPredicate.ReferencedColumnIndexes.Concat(RightPredicate.ReferencedColumnIndexes);

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => LeftPredicate.LeafPredicates.Concat(RightPredicate.LeafPredicates);

        internal override QueryPredicate? Simplify()
        {
            if (LeftPredicate.Equals(AllInPredicate.Instance))
            {
                return RightPredicate;
            }
            else if (RightPredicate.Equals(AllInPredicate.Instance))
            {
                return LeftPredicate;
            }
            else if (LeftPredicate is ResultPredicate rpl && RightPredicate is ResultPredicate rpr)
            {
                return new ResultPredicate(rpl.RecordIndexes.Intersect(rpr.RecordIndexes));
            }
            else
            {
                var sl = LeftPredicate.Simplify();
                var sr = RightPredicate.Simplify();

                if (sl != null || sr != null)
                {
                    var simplified =
                        new ConjunctionPredicate(sl ?? LeftPredicate, sr ?? RightPredicate);

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
                    ? new ConjunctionPredicate(sl ?? LeftPredicate, sr ?? RightPredicate)
                    : null;
            }
        }

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            return new ConjunctionPredicate(
                LeftPredicate.TransformToMetadata(correspondanceMap),
                RightPredicate.TransformToMetadata(correspondanceMap));
        }

        public override string ToString()
        {
            return $"({LeftPredicate}) && ({RightPredicate})";
        }
    }
}
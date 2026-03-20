using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

        /// <summary>Essentially implements De Morgan's Laws.</summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        internal override QueryPredicate? Simplify()
        {
            BinaryOperator Negate(BinaryOperator bo)
            {
                return bo switch
                {
                    BinaryOperator.Equal => BinaryOperator.NotEqual,
                    BinaryOperator.NotEqual => BinaryOperator.Equal,
                    BinaryOperator.LessThan => BinaryOperator.GreaterThanOrEqual,
                    BinaryOperator.LessThanOrEqual => BinaryOperator.GreaterThan,
                    BinaryOperator.GreaterThan => BinaryOperator.LessThanOrEqual,
                    BinaryOperator.GreaterThanOrEqual => BinaryOperator.LessThan,
                    _ => throw new NotSupportedException($"{nameof(BinaryOperator)} '{bo}'")
                };
            }

            switch (InnerPredicate)
            {
                case NegationPredicate np:
                    return np.InnerPredicate;
                case BinaryOperatorPredicate bop:
                    return bop with { BinaryOperator = Negate(bop.BinaryOperator) };
                case IInPredicate ip:
                    return ip.InverseIsIn();
                case ConjunctionPredicate cp:
                    {
                        var newLeft = new NegationPredicate(cp.LeftPredicate);
                        var newRight = new NegationPredicate(cp.RightPredicate);

                        return new DisjunctionPredicate(
                            newLeft.Simplify() ?? newLeft,
                            newRight.Simplify() ?? newRight);
                    }
                case DisjunctionPredicate cp:
                    {
                        var newLeft = new NegationPredicate(cp.LeftPredicate);
                        var newRight = new NegationPredicate(cp.RightPredicate);

                        return new ConjunctionPredicate(
                            newLeft.Simplify() ?? newLeft,
                            newRight.Simplify() ?? newRight);
                    }
                default:
                    throw new NotSupportedException(
                        $"Negating predicate type {InnerPredicate.GetType().Name}");
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
                var si = InnerPredicate.Substitute(beforePredicate, afterPredicate);

                return si != null
                    ? new NegationPredicate(si)
                    : null;
            }
        }

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            return new NegationPredicate(InnerPredicate.TransformToMetadata(correspondanceMap));
        }

        public override string ToString()
        {
            return $"!({InnerPredicate})";
        }
    }
}
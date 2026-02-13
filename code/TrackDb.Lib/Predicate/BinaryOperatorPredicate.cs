using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public sealed record BinaryOperatorPredicate(
        int ColumnIndex,
        object? Value,
        BinaryOperator BinaryOperator)
        : QueryPredicate
    {
        internal override IEnumerable<int> ReferencedColumnIndexes => [ColumnIndex];

        internal override IEnumerable<QueryPredicate> LeafPredicates
        {
            get
            {
                yield return this;
            }
        }

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is BinaryOperatorPredicate bop
                && ColumnIndex == bop.ColumnIndex
                && Value == bop.Value
                && BinaryOperator == bop.BinaryOperator;
        }

        internal override QueryPredicate? Simplify() => null;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            if (Value == null)
            {
                return AllInPredicate.Instance;
            }
            else
            {
                var correspondance = correspondanceMap[ColumnIndex];

                switch (BinaryOperator)
                {
                    case BinaryOperator.Equal:
                        //  For x==a => min_x<=a && max_x>=a
                        return new ConjunctionPredicate(
                            new BinaryOperatorPredicate(
                                correspondance.MetaMinColumnIndex,
                                Value,
                                BinaryOperator.LessThanOrEqual),
                            new NegationPredicate(
                                new BinaryOperatorPredicate(
                                    correspondance.MetaMaxColumnIndex,
                                    Value,
                                    BinaryOperator.LessThan)));
                    case BinaryOperator.LessThan:
                        //  For x<a => max_x<a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMaxColumnIndex,
                            Value,
                            BinaryOperator.LessThan);
                    case BinaryOperator.LessThanOrEqual:
                        //  For x<=a => max_x<=a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMaxColumnIndex,
                            Value,
                            BinaryOperator.LessThanOrEqual);
                    default:
                        throw new NotSupportedException($"Binary operation '{BinaryOperator}'");
                }
            }
        }

        public override string ToString()
        {
            var operatorText = BinaryOperator switch
            {
                BinaryOperator.Equal => "==",
                BinaryOperator.LessThan => "<",
                BinaryOperator.LessThanOrEqual => "<=",
                _ => throw new NotSupportedException(BinaryOperator.ToString())
            };

            return $"v[{ColumnIndex}] {operatorText} {Value}";
        }
    }
}
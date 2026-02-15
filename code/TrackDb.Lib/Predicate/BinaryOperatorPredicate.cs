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
                            new BinaryOperatorPredicate(
                                correspondance.MetaMaxColumnIndex,
                                Value,
                                BinaryOperator.GreaterThanOrEqual));
                    case BinaryOperator.NotEqual:
                        //  For x!=a => min_x!=a || max_x!=a
                        return new DisjunctionPredicate(
                            new BinaryOperatorPredicate(
                                correspondance.MetaMinColumnIndex,
                                Value,
                                BinaryOperator.NotEqual),
                            new BinaryOperatorPredicate(
                                correspondance.MetaMaxColumnIndex,
                                Value,
                                BinaryOperator.NotEqual));
                    case BinaryOperator.LessThan:
                        //  For x<a => min_x<a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMinColumnIndex,
                            Value,
                            BinaryOperator.LessThan);
                    case BinaryOperator.LessThanOrEqual:
                        //  For x<=a => min_x<=a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMinColumnIndex,
                            Value,
                            BinaryOperator.LessThanOrEqual);
                    case BinaryOperator.GreaterThan:
                        //  For x>a => max_x>a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMaxColumnIndex,
                            Value,
                            BinaryOperator.GreaterThan);
                    case BinaryOperator.GreaterThanOrEqual:
                        //  For x>=a => max_x>=a
                        return new BinaryOperatorPredicate(
                            correspondance.MetaMaxColumnIndex,
                            Value,
                            BinaryOperator.GreaterThanOrEqual);
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
                BinaryOperator.NotEqual => "!=",
                BinaryOperator.LessThan => "<",
                BinaryOperator.LessThanOrEqual => "<=",
                BinaryOperator.GreaterThan => ">",
                BinaryOperator.GreaterThanOrEqual => ">=",
                _ => throw new NotSupportedException(BinaryOperator.ToString())
            };

            return $"v[{ColumnIndex}] {operatorText} {Value}";
        }
    }
}
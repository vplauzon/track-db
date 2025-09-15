using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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
        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is BinaryOperatorPredicate bop
                && ColumnIndex == bop.ColumnIndex
                && Value == bop.Value
                && BinaryOperator == bop.BinaryOperator;
        }

        internal override IEnumerable<QueryPredicate> LeafPredicates
        {
            get
            {
                yield return this;
            }
        }

        internal override QueryPredicate? Simplify()=> null;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

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
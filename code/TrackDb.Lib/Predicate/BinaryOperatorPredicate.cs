using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal record BinaryOperatorPredicate(
        int ColumnIndex,
        object? Value,
        BinaryOperator BinaryOperator)
        : IQueryPredicate
    {
        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is BinaryOperatorPredicate bop
                && bop.ColumnIndex == bop.ColumnIndex
                && bop.Value == bop.Value
                && bop.BinaryOperator == bop.BinaryOperator;
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates
        {
            get
            {
                yield return this;
            }
        }

        IQueryPredicate? IQueryPredicate.Simplify() => null;

        IQueryPredicate? IQueryPredicate.Substitute(
            IQueryPredicate beforePredicate,
            IQueryPredicate afterPredicate) => beforePredicate.Equals(this) ? afterPredicate : null;
    }
}
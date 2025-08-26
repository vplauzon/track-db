using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace TrackDb.Lib.Predicate
{
    internal record InPredicate(int ColumnIndex, IImmutableSet<object?> Values)
        : IQueryPredicate
    {
        public InPredicate(int ColumnIndex, IEnumerable<object?> Values)
            : this(ColumnIndex, Values.ToImmutableHashSet())
        {
        }

        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is InPredicate ip
                && ip.ColumnIndex == ColumnIndex
                && ip.Values == Values;
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
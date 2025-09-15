using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Predicate
{
    public sealed record InPredicate(int ColumnIndex, IImmutableSet<object?> Values)
        : QueryPredicate
    {
        public InPredicate(int ColumnIndex, IEnumerable<object?> Values)
            : this(ColumnIndex, Values.ToImmutableHashSet())
        {
        }

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is InPredicate ip
                && ip.ColumnIndex == ColumnIndex
                && ip.Values == Values;
        }

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

        public override string ToString()
        {
            var set = string.Join(", ", Values.OrderBy(v => v));

            return $"v[{ColumnIndex}] in {{{set}}}";
        }
    }
}
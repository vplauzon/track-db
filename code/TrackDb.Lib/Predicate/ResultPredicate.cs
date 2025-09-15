using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal sealed record ResultPredicate : QueryPredicate
    {
        public ResultPredicate(IEnumerable<int> recordIndexes)
        {
            RecordIndexes = recordIndexes.ToImmutableHashSet();
        }

        public IImmutableSet<int> RecordIndexes { get; }

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is ResultPredicate rp
                && rp.RecordIndexes.SetEquals(RecordIndexes);
        }

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => Array.Empty<QueryPredicate>();

        internal override QueryPredicate? Simplify() => null;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

        public override string ToString()
        {
            return $"{{{string.Join(", ", RecordIndexes)}}}";
        }
    }
}
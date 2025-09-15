using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public sealed record AllInPredicate : QueryPredicate
    {
        private AllInPredicate()
        {
        }

        public static AllInPredicate Instance { get; } = new AllInPredicate();

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is AllInPredicate;
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
            return "All";
        }
    }
}
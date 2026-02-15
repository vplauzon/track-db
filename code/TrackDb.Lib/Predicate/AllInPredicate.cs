using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

        internal override IEnumerable<int> ReferencedColumnIndexes => Array.Empty<int>();

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => Array.Empty<QueryPredicate>();

        internal override QueryPredicate? Simplify() => null;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            return this;
        }

        public override string ToString()
        {
            return "All";
        }
    }
}
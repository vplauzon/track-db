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
            RecordIndexes = recordIndexes.ToHashSet();
        }

        public static ResultPredicate Empty { get; } = new ResultPredicate(Array.Empty<int>());

        public ISet<int> RecordIndexes { get; }

        internal override IEnumerable<int> ReferencedColumnIndexes => Array.Empty<int>();

        internal override IEnumerable<QueryPredicate> LeafPredicates
            => Array.Empty<QueryPredicate>();

        internal override bool PredicateEquals(QueryPredicate? other)
        {
            return other is ResultPredicate rp
                && rp.RecordIndexes.SetEquals(RecordIndexes);
        }

        internal override QueryPredicate? Simplify() => null;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            if (RecordIndexes.Count == 0)
            {
                return this;
            }
            else
            {
                throw new InvalidOperationException();
            }
        }

        public override string ToString()
        {
            return $"Result:  {{{string.Join(", ", RecordIndexes)}}}";
        }
    }
}
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
            RecordIndexes = recordIndexes.ToImmutableArray();
        }

        public static ResultPredicate Empty { get; } = new ResultPredicate(Array.Empty<int>());

        public IImmutableList<int> RecordIndexes { get; }

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
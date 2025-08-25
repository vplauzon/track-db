using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal class ResultPredicate : IQueryPredicate
    {
        public ResultPredicate(IEnumerable<int> recordIndexes)
        {
            RecordIndexes = recordIndexes.ToImmutableHashSet();
        }

        public IImmutableSet<int> RecordIndexes { get; }

        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is ResultPredicate rp
                && rp.RecordIndexes.SetEquals(RecordIndexes);
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates => Array.Empty<IQueryPredicate>();

        IQueryPredicate? IQueryPredicate.Simplify() => null;

        IQueryPredicate? IQueryPredicate.Substitute(
            IQueryPredicate beforePredicate,
            IQueryPredicate afterPredicate) => beforePredicate.Equals(this) ? afterPredicate : null;
    }
}
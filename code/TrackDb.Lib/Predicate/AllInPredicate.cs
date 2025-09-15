using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal class AllInPredicate : IQueryPredicate
    {
        private AllInPredicate()
        {
        }

        public static AllInPredicate Instance { get; } = new AllInPredicate();

        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is AllInPredicate;
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates => Array.Empty<IQueryPredicate>();

        IQueryPredicate? IQueryPredicate.Simplify() => null;

        IQueryPredicate? IQueryPredicate.Substitute(
            IQueryPredicate beforePredicate,
            IQueryPredicate afterPredicate) => beforePredicate.Equals(this) ? afterPredicate : null;

        public override string ToString()
        {
            return "All";
        }
    }
}
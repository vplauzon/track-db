using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public abstract record QueryPredicate : IEquatable<QueryPredicate>
    {
        bool IEquatable<QueryPredicate>.Equals(QueryPredicate? other)
        {
            return PredicateEquals(other);
        }

        /// <summary>Column indexes used in the leaf predicates.</summary>
        internal abstract IEnumerable<int> ReferencedColumnIndexes { get; }

        /// <summary>Leaf predicates in the chain.</summary>
        /// <remarks>
        /// A leaf predicate is a predicate that can be resolved into a
        /// <see cref="ResultPredicate"/>.
        /// The main exception is <see cref="AllInPredicate"/>.
        /// </remarks>
        internal abstract IEnumerable<QueryPredicate> LeafPredicates { get; }

        /// <summary>Compares two predicates.</summary>
        /// <param name="other"></param>
        /// <returns></returns>
        internal abstract bool PredicateEquals(QueryPredicate? other);

        /// <summary>Applies any simplification rules.</summary>
        /// <returns>Simplified predicate (or <c>null</c> if unchanged).</returns>
        internal abstract QueryPredicate? Simplify();

        /// <summary>Substitute a predicate for another.</summary>
        /// <param name="beforePredicate"></param>
        /// <param name="afterPredicate"></param>
        /// <returns>Substituted predicate (or <c>null</c> if unchanged).</returns>
        internal abstract QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate);
    }
}
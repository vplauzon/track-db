using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public interface IQueryPredicate : IEquatable<IQueryPredicate>
    {
        /// <summary>Leaf predicates in the chain.</summary>
        /// <remarks>
        /// A leaf predicate is a predicate that can be resolved into a
        /// <see cref="ResultPredicate"/>.
        /// The main exception is <see cref="AllInPredicate"/>.
        /// </remarks>
        IEnumerable<IQueryPredicate> LeafPredicates { get; }

        /// <summary>Applies any simplification rules.</summary>
        /// <returns>Simplified predicate (or <c>null</c> if unchanged).</returns>
        IQueryPredicate? Simplify();

        /// <summary>Substitute a predicate for another.</summary>
        /// <param name="beforePredicate"></param>
        /// <param name="afterPredicate"></param>
        /// <returns>Substituted predicate (or <c>null</c> if unchanged).</returns>
        IQueryPredicate? Substitute(IQueryPredicate beforePredicate, IQueryPredicate afterPredicate);
    }
}
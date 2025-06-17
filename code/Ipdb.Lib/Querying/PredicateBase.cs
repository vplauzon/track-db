using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public abstract class PredicateBase<T>
    {
        /// <summary>
        /// Represents the first (the left-most) primitive predicate in the graph.
        /// It's the first predicate expression we should resolve.
        /// </summary>
        /// <remarks>
        /// If it is <c>null</c> it means we have a result (i.e. a list of revision IDs).
        /// </remarks>
        internal abstract PredicateBase<T>? FirstPrimitivePredicate { get; }

        /// <summary>
        /// This should return a simplified version of the graph where
        /// a primitive predicate has been resolved.
        /// </summary>
        /// <param name="primitive"></param>
        /// <param name="revisionIds"></param>
        /// <returns><c>null</c> means the predicate remains unchanged.</returns>
        internal abstract PredicateBase<T>? Simplify(
            PredicateBase<T> primitive,
            IImmutableSet<long> revisionIds);

        /// <summary>
        /// Execute the predicate on actual documents.
        /// This allows to remove hash clash and filter down the document
        /// list to those that truely satisfy the predicates (not its hash approximation).
        /// </summary>
        /// <param name="documents"></param>
        /// <returns></returns>
        internal abstract IEnumerable<T> FilterDocuments(IEnumerable<T> documents);
    }
}

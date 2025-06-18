using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Ipdb.Lib.Querying
{
    /// <summary>
    /// Represents an actual result instead of a predicate.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class ResultPredicate<T> : PredicateBase<T>
    {
        public ResultPredicate(IImmutableSet<long> revisionIds)
        {
            RevisionIds = revisionIds;
        }

        public IImmutableSet<long> RevisionIds { get; }

        internal override PredicateBase<T>? FirstPrimitivePredicate => null;

        internal override PredicateBase<T>? Simplify(
            PredicateBase<T> primitive,
            IImmutableSet<long> revisionIds) => null;

        internal override IEnumerable<DocumentRevision<T>> FilterRevisionDocuments(
            IEnumerable<DocumentRevision<T>> documents)
        {
            throw new NotSupportedException("This should never be invoked");
        }
    }
}
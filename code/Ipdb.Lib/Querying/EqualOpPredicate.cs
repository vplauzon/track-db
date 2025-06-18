using Ipdb.Lib.Indexing;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public class EqualOpPredicate<T, PT> : PredicateBase<T>, IIndexEqual<T>
    {
        private readonly IndexDefinition<T> _indexDefinition;
        private readonly PT _propertyValue;

        #region Constructor
        internal EqualOpPredicate(
            IImmutableDictionary<string, IndexDefinition<T>> indexMap,
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
        {
            var propertyPath = propertyExtractor.ToPath();

            if (indexMap.TryGetValue(propertyPath, out var indexDefinition))
            {
                _indexDefinition = indexDefinition;
            }
            else
            {
                throw new ArgumentException(
                    $"Can't find index with property path '{propertyPath}'",
                    nameof(propertyPath));
            }
            _propertyValue = propertyValue;
        }
        #endregion

        IndexDefinition<T> IIndexEqual<T>.IndexDefinition => _indexDefinition;

        short IIndexEqual<T>.KeyHash =>
            ((Func<PT, short>)_indexDefinition.HashFunc)(_propertyValue);

        internal override PredicateBase<T>? FirstPrimitivePredicate => this;

        internal override PredicateBase<T>? Simplify(
            PredicateBase<T> primitive,
            IImmutableSet<long> revisionIds)
        {
            if (primitive == this)
            {
                return new ResultPredicate<T>(revisionIds);
            }
            else
            {
                return null;
            }
        }

        internal override IEnumerable<DocumentRevision<T>> FilterRevisionDocuments(
            IEnumerable<DocumentRevision<T>> documents)
        {
            return documents
                .Where(d => object.Equals(
                    _indexDefinition.KeyExtractor(d.Document),
                    _propertyValue));
        }
    }
}

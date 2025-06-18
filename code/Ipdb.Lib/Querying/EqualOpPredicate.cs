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
    public class EqualOpPredicate<T> : PredicateBase<T>, IIndexEqual<T>
    {
        private readonly IndexDefinition<T> _indexDefinition;
        private readonly object? _propertyValue;
        private readonly short _hashValue;

        #region Constructor
        internal EqualOpPredicate(
            IndexDefinition<T> indexDefinition,
            object? propertyValue,
            short hashValue)
        {
            _indexDefinition = indexDefinition;
            _propertyValue = propertyValue;
            _hashValue = hashValue;
        }
        #endregion

        IndexDefinition<T> IIndexEqual<T>.IndexDefinition => _indexDefinition;

        short IIndexEqual<T>.KeyHash => _hashValue;

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

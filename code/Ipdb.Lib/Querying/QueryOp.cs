using Ipdb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public class QueryOp<T>
    {
        private readonly IImmutableDictionary<string, IndexDefinition<T>> _indexMap;

        internal QueryOp(IImmutableDictionary<string, IndexDefinition<T>> indexMap)
        {
            _indexMap = indexMap;
        }

        public EqualOpPredicate<T> Equal<PT>(
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
            where PT : notnull
        {
            var propertyPath = propertyExtractor.ToPath();

            if (_indexMap.TryGetValue(propertyPath, out var indexDefinition))
            {
                var hashValue = ((Func<PT, short>)indexDefinition.HashFunc)(propertyValue);

                return new EqualOpPredicate<T>(indexDefinition, propertyValue, hashValue);
            }
            else
            {
                throw new ArgumentException(
                    $"Can't find index with property path '{propertyPath}'",
                    nameof(propertyPath));
            }
        }
    }
}
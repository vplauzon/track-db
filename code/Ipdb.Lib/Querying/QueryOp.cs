using Ipdb.Lib.Indexing;
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

        public EqualOpPredicate<T, PT> Equal<PT>(
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
        {
            return new EqualOpPredicate<T, PT>(_indexMap, propertyExtractor, propertyValue);
        }
    }
}
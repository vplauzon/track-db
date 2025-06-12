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
        private readonly IImmutableDictionary<Expression, Func<T, IndexValues>> _indexMap;

        internal QueryOp(IImmutableDictionary<Expression, Func<T, IndexValues>> indexMap)
        {
            _indexMap = indexMap;
        }

        public EqualOp<T, PT> Equal<PT>(
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
        {
            return new EqualOp<T, PT>(_indexMap, propertyExtractor, propertyValue);
        }
    }
}
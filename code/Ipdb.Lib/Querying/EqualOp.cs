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
    public class EqualOp<T, PT> : QueryPredicate<T>
    {
        private readonly Expression<Func<T, PT>> _propertyExpression;
        private readonly Func<T, PT> _propertyExtractor;
        private readonly PT _propertyValue;

        internal EqualOp(
            IImmutableDictionary<Expression, Func<T, IndexValues>> indexMap,
            Expression<Func<T, PT>> propertyExpression,
            PT propertyValue)
        {
            _propertyExpression = propertyExpression;
            if(indexMap.TryGetValue(_propertyExpression, out var extractor))
            {
                if(extractor is Func<T, PT> pe)
                {
                    _propertyExtractor = pe;
                }
                else
                {
                    throw new ArgumentException(
                        "Wrong property type",
                        nameof(propertyExpression));
                }
            }
            else
            {
                throw new ArgumentException("Can't find property", nameof(propertyExpression));
            }
            _propertyValue = propertyValue;
        }

        internal override IImmutableList<Expression> GetProperties()
        {
            return ImmutableList.Create(_propertyExpression.Body);
        }

        internal override IImmutableList<short> CombineHash(
            IImmutableDictionary<Expression, IImmutableList<short>> hashMap)
        {
            if (hashMap.TryGetValue(_propertyExpression, out var hashList))
            {
                return hashList;
            }
            else
            {
                throw new ArgumentException($"Can't find expression", nameof(hashMap));
            }
        }

        internal override IImmutableList<T> FilterDocuments(IImmutableList<T> documents)
        {
            throw new NotImplementedException();
        }
    }
}

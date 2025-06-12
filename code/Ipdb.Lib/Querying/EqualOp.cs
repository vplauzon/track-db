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
    public record EqualOp<T, PT>(
        Expression<Func<T, PT>> propertyExtractor,
        PT propertyValue) : QueryPredicate<T>
    {
        internal override IImmutableList<Expression> GetProperties()
        {
            return ImmutableList.Create(propertyExtractor.Body);
        }

        internal override IImmutableList<short> CombineHash(
            IImmutableDictionary<Expression, IImmutableList<short>> hashMap)
        {
            if (hashMap.TryGetValue(propertyExtractor, out var hashList))
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
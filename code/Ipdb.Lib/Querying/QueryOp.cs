using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public static class QueryOp
    {
        public static EqualOp<T, PT> Equal<T, PT>(
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
        {
            return new EqualOp<T, PT>(propertyExtractor, propertyValue);
        }
    }
}
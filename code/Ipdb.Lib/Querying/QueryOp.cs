using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public class QueryOp<T>
    {
        public EqualOp<T, PT> Equal<PT>(
            Expression<Func<T, PT>> propertyExtractor,
            PT propertyValue)
        {
            return new EqualOp<T, PT>(propertyExtractor, propertyValue);
        }
    }
}
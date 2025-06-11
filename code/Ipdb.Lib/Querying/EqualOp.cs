using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    public record EqualOp<T, PT>(
        Expression<Func<T, PT>> propertyExtractor,
        PT propertyValue) : QueryPredicate<T>
    {
    }
}
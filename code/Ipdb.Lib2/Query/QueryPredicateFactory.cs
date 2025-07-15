using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal class QueryPredicateFactory
    {
        public static IQueryPredicate Create<T>(
            Expression<Func<T, bool>> predicateExpression)
        {
            throw new NotImplementedException();
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public static class QueryPredicateHelper
    {
        public static ITypedQueryPredicate<T> Not<T>(ITypedQueryPredicate<T> predicate)
            where T : notnull
        {
            throw new NotImplementedException();
        }

        public static ITypedQueryPredicate<T> And<T>(
            this ITypedQueryPredicate<T> predicate1,
            ITypedQueryPredicate<T> predicate2)
            where T : notnull
        {
            throw new NotImplementedException();
        }

        public static ITypedQueryPredicate<T> Or<T>(
            this ITypedQueryPredicate<T> predicate1,
            ITypedQueryPredicate<T> predicate2)
            where T : notnull
        {
            throw new NotImplementedException();
        }
    }
}
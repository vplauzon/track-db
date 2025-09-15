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
        public static ITypedQueryPredicate<T> Not<T>(this ITypedQueryPredicate<T> predicate)
            where T : notnull
        {
            return new TypedQueryPredicateAdapter<T>(new NegationPredicate(predicate));
        }

        public static ITypedQueryPredicate<T> And<T>(
            this ITypedQueryPredicate<T> leftPredicate,
            ITypedQueryPredicate<T> rightPredicate)
            where T : notnull
        {
            return new TypedQueryPredicateAdapter<T>(
                new ConjunctionPredicate(leftPredicate, rightPredicate));
        }

        public static ITypedQueryPredicate<T> Or<T>(
            this ITypedQueryPredicate<T> leftPredicate,
            ITypedQueryPredicate<T> rightPredicate)
            where T : notnull
        {
            return new TypedQueryPredicateAdapter<T>(
                new DisjunctionPredicate(leftPredicate, rightPredicate));
        }
    }
}
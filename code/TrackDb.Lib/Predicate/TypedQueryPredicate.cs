using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public sealed class TypedQueryPredicate<T>
        where T : notnull
    {
        internal TypedQueryPredicate(QueryPredicate queryPredicate, TypedTableSchema<T> schema)
        {
            QueryPredicate = queryPredicate;
            Schema = schema;
        }

        internal QueryPredicate QueryPredicate { get; }

        internal TypedTableSchema<T> Schema { get; }

        public override string ToString()
        {
            return QueryPredicate.ToString()!;
        }

        #region Extending query
        public TypedQueryPredicate<T> Not()
        {
            return new TypedQueryPredicate<T>(new NegationPredicate(QueryPredicate), Schema);
        }

        public TypedQueryPredicate<T> And(TypedQueryPredicate<T> rightPredicate)
        {
            return new TypedQueryPredicate<T>(
                new ConjunctionPredicate(QueryPredicate, rightPredicate.QueryPredicate),
                Schema);
        }

        public TypedQueryPredicate<T> Or(TypedQueryPredicate<T> rightPredicate)
        {
            return new TypedQueryPredicate<T>(
                new DisjunctionPredicate(QueryPredicate, rightPredicate.QueryPredicate),
                Schema);
        }
        #endregion
    }
}
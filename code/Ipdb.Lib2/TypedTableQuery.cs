using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
using Ipdb.Lib2.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;

namespace Ipdb.Lib2
{
    public class TypedTableQuery<T> : IEnumerable<T>
        where T : notnull
    {
        private readonly TypedTable<T> _table;
        private readonly TransactionContext? _transactionContext;
        private readonly IQueryPredicate _predicate;
        private readonly int? _takeCount;
        private IImmutableList<int> _projectionColumnIndexes;

        #region Constructors
        internal TypedTableQuery(TypedTable<T> table, TransactionContext? transactionContext)
            : this(table, transactionContext, new AllInPredicate(), null)
        {
        }

        internal TypedTableQuery(
            TypedTable<T> table,
            TransactionContext? transactionContext,
            IQueryPredicate predicate,
            int? takeCount)
        {
            _table = table;
            _transactionContext = transactionContext;
            _predicate = predicate;
            _takeCount = takeCount;
            _projectionColumnIndexes = Enumerable.Range(0, table.Schema.Columns.Count)
                .ToImmutableArray();
        }
        #endregion

        #region Query alteration
        public TypedTableQuery<T> Where(Expression<Func<T, bool>> predicate)
        {
            var queryPredicate = QueryPredicateFactory.Create(predicate, _table.Schema);
            var newQueryPredicate = new ConjunctionPredicate(_predicate, queryPredicate);

            return new TypedTableQuery<T>(_table, _transactionContext, newQueryPredicate, _takeCount);
        }

        public TypedTableQuery<T> Take(int count)
        {
            throw new NotImplementedException();
        }

        public TypedTableQuery<T> OrderBy<U>(Expression<Func<T, U>> propertySelector)
        {
            throw new NotImplementedException();
        }

        public TypedTableQuery<T> OrderByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return ExecuteQuery().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ExecuteQuery().GetEnumerator();
        }
        #endregion

        public long Count()
        {
            throw new NotImplementedException();
        }

        public void Delete()
        {
            var tableQuery = new TableQuery(_table, _transactionContext, _predicate, null, _takeCount);

            tableQuery.Delete();
        }

        #region Query internals
        private IEnumerable<T> ExecuteQuery()
        {
            var tableQuery = new TableQuery(_table, _transactionContext, _predicate, null, _takeCount);

            foreach(var result in tableQuery)
            {
                var objectRow = (T)_table.Schema.FromColumnsToObject(result.ToArray());

                yield return objectRow;
            }
        }
        #endregion
    }
}
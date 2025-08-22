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
        private readonly object?[] _rowBuffer;

        #region Constructors
        internal TypedTableQuery(TypedTable<T> table, TransactionContext? transactionContext)
            : this(table, transactionContext, AllInPredicate.Instance, null)
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
            _rowBuffer = new object?[_table.Schema.Columns.Count];
        }
        #endregion

        #region Query alteration
        public TypedTableQuery<T> Where(Expression<Func<T, bool>> predicate)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Where clause can't be added after a take");
            }

            var queryPredicate = QueryPredicateFactory.Create(predicate, _table.Schema);
            var newQueryPredicate = new ConjunctionPredicate(_predicate, queryPredicate);

            return new TypedTableQuery<T>(_table, _transactionContext, newQueryPredicate, _takeCount);
        }

        public TypedTableQuery<T> Take(int count)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Take clause can't be added after another take clause");
            }

            throw new NotImplementedException();
        }

        public TypedTableQuery<T> OrderBy<U>(Expression<Func<T, U>> propertySelector)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("OrderBy clause can't be added after a take");
            }

            throw new NotImplementedException();
        }

        public TypedTableQuery<T> OrderByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("OrderByDesc clause can't be added after a take");
            }

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
            var tableQuery = new TableQuery(_table, _transactionContext, _predicate, null, _takeCount);

            return tableQuery.Count();
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

            foreach (var result in tableQuery)
            {
                result.CopyTo(_rowBuffer);

                var objectRow = (T)_table.Schema.FromColumnsToObject(_rowBuffer);

                yield return objectRow;
            }
        }
        #endregion
    }
}
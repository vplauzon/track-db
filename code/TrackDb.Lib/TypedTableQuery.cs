using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.Predicate;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;

namespace TrackDb.Lib
{
    public class TypedTableQuery<T> : IEnumerable<T>
        where T : notnull
    {
        private readonly TypedTable<T> _table;
        private readonly TransactionContext? _transactionContext;
        private readonly IQueryPredicate _predicate;
        private readonly int? _takeCount;

        #region Constructors
        internal TypedTableQuery(TypedTable<T> table, TransactionContext? transactionContext)
            : this(
                  table,
                  transactionContext,
                  AllInPredicate.Instance,
                  null)
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
        }
        #endregion

        #region Query alteration
        public TypedTableQuery<T> Where(ITypedQueryPredicate<T> predicate)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Where clause can't be added after a take");
            }

            var newPredicate = new ConjunctionPredicate(_predicate, predicate);

            return new TypedTableQuery<T>(_table, _transactionContext, newPredicate, _takeCount);
        }

        public TypedTableQuery<T> Take(int count)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Take clause can't be added after another take clause");
            }

            throw new NotImplementedException();
        }

        #region Order By
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

        public TypedTableQuery<T> ThenBy<U>(Expression<Func<T, U>> propertySelector)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("OrderBy clause can't be added after a take");
            }

            throw new NotImplementedException();
        }

        public TypedTableQuery<T> ThenByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("OrderByDesc clause can't be added after a take");
            }

            throw new NotImplementedException();
        }
        #endregion
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
            var tableQuery = new TableQuery(
                _table,
                _transactionContext,
                _predicate,
                //  We are not bringing back any column
                Enumerable.Range(0, 0),
                _takeCount);

            return tableQuery.Count();
        }

        public void Delete()
        {
            var tableQuery = new TableQuery(
                _table,
                _transactionContext,
                _predicate,
                Enumerable.Range(0, _table.Schema.Columns.Count),
                _takeCount);

            tableQuery.Delete();
        }

        #region Query internals
        private IEnumerable<T> ExecuteQuery()
        {
            var columnCount = _table.Schema.Columns.Count;
            var tableQuery = new TableQuery(
                _table,
                _transactionContext,
                _predicate,
                Enumerable.Range(0, columnCount),
                _takeCount);
            var rowBuffer = new object?[columnCount];

            foreach (var result in tableQuery)
            {
                result.CopyTo(rowBuffer);

                var objectRow = (T)_table.Schema.FromColumnsToObject(rowBuffer);

                yield return objectRow;
            }
        }
        #endregion
    }
}
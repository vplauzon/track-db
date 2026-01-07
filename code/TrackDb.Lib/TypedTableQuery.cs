using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib
{
    public class TypedTableQuery<T> : IEnumerable<T>
        where T : notnull
    {
        private readonly TypedTable<T> _table;
        private readonly bool _canDelete;
        private readonly bool _inTxOnly;
        private readonly bool _committedOnly;
        private readonly TransactionContext? _transactionContext;
        private readonly IImmutableList<SortColumn> _sortColumns;
        private readonly int? _takeCount;
        private readonly string? _queryTag;

        #region Constructors
        internal TypedTableQuery(
            TypedTable<T> table,
            bool canDelete,
            TransactionContext? transactionContext)
            : this(
                  table,
                  canDelete,
                  false,
                  false,
                  transactionContext,
                  AllInPredicate.Instance,
                  ImmutableArray<SortColumn>.Empty,
                  null,
                  null)
        {
        }

        internal TypedTableQuery(
            TypedTable<T> table,
            bool canDelete,
            bool inTxOnly,
            bool committedOnly,
            TransactionContext? transactionContext,
            QueryPredicate predicate,
            IEnumerable<SortColumn> sortColumns,
            int? takeCount,
            string? queryTag)
        {
            _table = table;
            _canDelete = canDelete;
            _inTxOnly = inTxOnly;
            _committedOnly = committedOnly;
            _transactionContext = transactionContext;
            Predicate = predicate;
            _sortColumns = sortColumns.ToImmutableArray();
            _takeCount = takeCount;
            _queryTag = queryTag;
        }
        #endregion

        #region Query alteration
        public TypedTableQuery<T> Where(
            Func<QueryPredicateFactory<T>, TypedQueryPredicate<T>> predicateFunc)
        {
            if (_sortColumns.Any())
            {
                throw new InvalidOperationException("Where clause can't be added after an orderby");
            }
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Where clause can't be added after a take");
            }

            var predicate = predicateFunc(_table.PredicateFactory);
            var newPredicate = new ConjunctionPredicate(Predicate, predicate.QueryPredicate);

            return new TypedTableQuery<T>(
                _table,
                _canDelete,
                _inTxOnly,
                _committedOnly,
                _transactionContext,
                newPredicate,
                Array.Empty<SortColumn>(),
                _takeCount,
                _queryTag);
        }

        public TypedTableQuery<T> Take(int count)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Take clause can't be added after another take clause");
            }

            return new TypedTableQuery<T>(
                _table,
                _canDelete,
                _inTxOnly,
                _committedOnly,
                _transactionContext,
                Predicate,
                _sortColumns,
                count,
                _queryTag);
        }

        #region Order By
        public TypedTableQuery<T> OrderBy<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, true, true);
        }

        public TypedTableQuery<T> OrderByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, false, true);
        }

        public TypedTableQuery<T> ThenBy<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, true, false);
        }

        public TypedTableQuery<T> ThenByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, false, false);
        }

        private TypedTableQuery<T> AlterOrderBy<U>(
            Expression<Func<T, U>> propertySelector,
            bool isAscending,
            bool isFirst)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("OrderBy clause can't be added after a take");
            }
            if (isFirst && _sortColumns.Any())
            {
                throw new InvalidOperationException(
                    "Order by clause can't be added after another orderby, use 'ThenBy' instead");
            }
            if (!isFirst && !_sortColumns.Any())
            {
                throw new InvalidOperationException("ThenBy must come after an orderby");
            }

            var columnIndexSubset = _table.Schema.GetColumnIndexSubset(propertySelector);

            if (columnIndexSubset.Count == 1)
            {
                return new TypedTableQuery<T>(
                    _table,
                    _canDelete,
                    _inTxOnly,
                    _committedOnly,
                    _transactionContext,
                    Predicate,
                    _sortColumns.Add(new SortColumn(columnIndexSubset[0], isAscending)),
                    _takeCount,
                    _queryTag);
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(propertySelector),
                    $"Expression '{propertySelector}' isn't mapped to a column");
            }
        }
        #endregion

        public TypedTableQuery<T> WithQueryTag(string queryTag)
        {
            return new TypedTableQuery<T>(
                _table,
                _canDelete,
                _inTxOnly,
                _committedOnly,
                _transactionContext,
                Predicate,
                _sortColumns,
                _takeCount,
                queryTag);
        }

        public TypedTableQuery<T> WithinTransactionOnly()
        {
            return new TypedTableQuery<T>(
                _table,
                _canDelete,
                true,
                _committedOnly,
                _transactionContext,
                Predicate,
                _sortColumns,
                _takeCount,
                _queryTag);
        }

        internal TypedTableQuery<T> WithCommittedOnly()
        {
            return new TypedTableQuery<T>(
                _table,
                _canDelete,
                _inTxOnly,
                true,
                _transactionContext,
                Predicate,
                _sortColumns,
                _takeCount,
                _queryTag);
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

        public QueryPredicate Predicate { get; }

        public TableQuery TableQuery
        {
            get
            {
                var tableQuery = ((Table)_table).Query(_transactionContext)
                    .WithPredicate(Predicate)
                    .WithSortColumns(_sortColumns)
                    .WithTake(_takeCount);

                if (_inTxOnly)
                {
                    tableQuery = tableQuery
                        .WithinTransactionOnly();
                }
                if (_queryTag != null)
                {
                    tableQuery = tableQuery.WithQueryTag(_queryTag);
                }

                return tableQuery;
            }
        }

        public bool Any()
        {
            return TableQuery.Any();
        }

        public long Count()
        {
            return TableQuery.Count();
        }

        public int Delete()
        {
            return TableQuery.Delete();
        }

        #region Query internals
        private IEnumerable<T> ExecuteQuery()
        {
            var columnCount = _table.Schema.Columns.Count;

            foreach (var result in TableQuery)
            {
                var objectRow = (T)_table.Schema.FromColumnsToObject(result.Span);

                yield return objectRow;
            }
        }
        #endregion
    }
}
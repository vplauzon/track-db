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
        private readonly QueryPredicate _predicate;
        private readonly IImmutableList<SortColumn> _sortColumns;
        private readonly int? _takeCount;

        #region Constructors
        internal TypedTableQuery(TypedTable<T> table, TransactionContext? transactionContext)
            : this(
                  table,
                  transactionContext,
                  AllInPredicate.Instance,
                  ImmutableArray<SortColumn>.Empty,
                  null)
        {
        }

        internal TypedTableQuery(
            TypedTable<T> table,
            TransactionContext? transactionContext,
            QueryPredicate predicate,
            IEnumerable<SortColumn> sortColumns,
            int? takeCount)
        {
            _table = table;
            _transactionContext = transactionContext;
            _predicate = predicate;
            _sortColumns = sortColumns.ToImmutableArray();
            _takeCount = takeCount;
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
            var newPredicate = new ConjunctionPredicate(_predicate, predicate.QueryPredicate);

            return new TypedTableQuery<T>(
                _table,
                _transactionContext,
                newPredicate,
                Array.Empty<SortColumn>(),
                _takeCount);
        }

        public TypedTableQuery<T> Take(int count)
        {
            if (_takeCount != null)
            {
                throw new InvalidOperationException("Take clause can't be added after another take clause");
            }

            return new TypedTableQuery<T>(
                _table,
                _transactionContext,
                _predicate,
                _sortColumns,
                count);
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

            var columnIndexSubset = _table.Schema.GetColumnIndexSubset(propertySelector.Body);

            if (columnIndexSubset.Count == 1)
            {
                return new TypedTableQuery<T>(
                    _table,
                    _transactionContext,
                    _predicate,
                    _sortColumns.Add(new SortColumn(columnIndexSubset[0], isAscending)),
                    _takeCount);
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(propertySelector),
                    $"Expression '{propertySelector}' isn't mapped to a column");
            }
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
                Enumerable.Range(0, 0))
                .WithTake(_takeCount);

            return tableQuery.Count();
        }

        public void Delete()
        {
            var tableQuery = new TableQuery(
                _table,
                _transactionContext,
                _predicate,
                Enumerable.Range(0, _table.Schema.Columns.Count))
                .WithSortColumns(_sortColumns)
                .WithTake(_takeCount);

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
                Enumerable.Range(0, columnCount))
                .WithSortColumns(_sortColumns)
                .WithTake(_takeCount);

            foreach (var result in tableQuery)
            {
                var objectRow = (T)_table.Schema.FromColumnsToObject(result.Span);

                yield return objectRow;
            }
        }
        #endregion
    }
}
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
        private readonly TableQuery _tableQuery;

        internal TypedTableQuery(TableQuery tableQuery)
        {
            _tableQuery = tableQuery;
        }

        private TypedTable<T> QueryTable => (TypedTable<T>)_tableQuery.QueryTable;

        #region Query alteration
        public TypedTableQuery<T> Where(
            Func<QueryPredicateFactory<T>, TypedQueryPredicate<T>> predicateFunc)
        {
            if (_tableQuery.SortColumns.Count > 0)
            {
                throw new InvalidOperationException("Where clause can't be added after an orderby");
            }
            if (_tableQuery.TakeCount != null)
            {
                throw new InvalidOperationException("Where clause can't be added after a take");
            }

            var predicate = predicateFunc(QueryTable.PredicateFactory);
            var newPredicate = new ConjunctionPredicate(Predicate, predicate.QueryPredicate);

            return new TypedTableQuery<T>(_tableQuery.WithPredicate(newPredicate));
        }

        public TypedTableQuery<T> Take(int count)
        {
            if (_tableQuery.TakeCount != null)
            {
                throw new InvalidOperationException("Take clause can't be added after another take clause");
            }

            return new TypedTableQuery<T>(_tableQuery.WithTake(count));
        }

        #region Order By
        public TypedTableQuery<T> OrderBy<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, true, true);
        }

        public TypedTableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, false, true);
        }

        public TypedTableQuery<T> ThenBy<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, true, false);
        }

        public TypedTableQuery<T> ThenByDescending<U>(Expression<Func<T, U>> propertySelector)
        {
            return AlterOrderBy(propertySelector, false, false);
        }

        private TypedTableQuery<T> AlterOrderBy<U>(
            Expression<Func<T, U>> propertySelector,
            bool isAscending,
            bool isFirst)
        {
            if (_tableQuery.TakeCount != null)
            {
                throw new InvalidOperationException("OrderBy clause can't be added after a take");
            }
            if (isFirst && _tableQuery.SortColumns.Count > 0)
                {
                    throw new InvalidOperationException(
                    "Order by clause can't be added after another orderby, use 'ThenBy' instead");
            }
            if (!isFirst && _tableQuery.SortColumns.Count == 0)
            {
                throw new InvalidOperationException("ThenBy must come after an orderby");
            }

            var columnIndexSubset = QueryTable.Schema.GetColumnIndexSubset(propertySelector);

            if (columnIndexSubset.Count == 1)
            {
                return new TypedTableQuery<T>(
                    _tableQuery.WithSortColumns(new SortColumn(columnIndexSubset[0], isAscending)));
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
            return new TypedTableQuery<T>(_tableQuery.WithQueryTag(queryTag));
        }

        public TypedTableQuery<T> WithinTransactionOnly()
        {
            return new TypedTableQuery<T>(_tableQuery.WithinTransactionOnly());
        }

        internal TypedTableQuery<T> WithCommittedOnly()
        {
            return new TypedTableQuery<T>(_tableQuery.WithCommittedOnly());
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

        public QueryPredicate Predicate => _tableQuery.Predicate;

        public TableQuery TableQuery => _tableQuery;

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
            var columnCount = _tableQuery.QueryTable.Schema.Columns.Count;
            var queryTable = QueryTable;

            foreach (var result in TableQuery)
            {
                var objectRow = (T)queryTable.Schema.FromColumnsToObject(result.Span);

                yield return objectRow;
            }
        }
        #endregion
    }
}
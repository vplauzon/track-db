using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
using Ipdb.Lib2.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Ipdb.Lib2
{
    public class TableQuery<T> : IEnumerable<T>
        where T : notnull
    {
        private readonly Table<T> _table;
        private readonly TransactionContext? _transactionContext;
        private readonly IQueryPredicate _predicate;
        private readonly int? _takeCount;

        #region Constructors
        internal TableQuery(Table<T> table, TransactionContext? transactionContext)
            : this(table, transactionContext, new AllInPredicate(), null)
        {
        }

        internal TableQuery(
            Table<T> table,
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
        public TableQuery<T> Where(Expression<Func<T, bool>> predicate)
        {
            var queryPredicate = QueryPredicateFactory.Create(predicate);

            throw new NotImplementedException();
        }

        public TableQuery<T> Take(int count)
        {
            throw new NotImplementedException();
        }

        public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> propertySelector)
        {
            throw new NotImplementedException();
        }

        public TableQuery<T> OrderByDesc<U>(Expression<Func<T, U>> propertySelector)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return ExecuteQuery();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ExecuteQuery();
        }
        #endregion

        public long Count()
        {
            throw new NotImplementedException();
        }

        #region Query internals
        private IEnumerator<T> ExecuteQuery()
        {
            var result = _table.Database.ExecuteWithinTransactionContext(
                _transactionContext,
                transactionCache =>
                {
                    return ExecuteQuery(transactionCache);
                });

            return result;
        }

        private IEnumerator<T> ExecuteQuery(TransactionCache transactionCache)
        {
            IBlock txBlock =
                transactionCache.TransactionLog.TableTransactionLogMap[_table.Name].BlockBuilder;
            var recordIds = txBlock.Query(_predicate);

            throw new NotImplementedException();
        }
        #endregion
    }
}
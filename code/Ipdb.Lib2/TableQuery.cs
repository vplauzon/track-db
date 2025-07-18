using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
using Ipdb.Lib2.DbStorage;
using Ipdb.Lib2.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Net.NetworkInformation;

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
            var newQueryPredicate = new ConjunctionPredicate(_predicate, queryPredicate);

            return new TableQuery<T>(_table, _transactionContext, newQueryPredicate, _takeCount);
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

        #region Query internals
        private IEnumerable<T> ExecuteQuery()
        {
            var result = _table.Database.ExecuteWithinTransactionContext(
                _transactionContext,
                transactionCache =>
                {
                    if (_takeCount == 0)
                    {
                        return Array.Empty<T>();
                    }
                    else
                    {
                        return ExecuteQuery(transactionCache);
                    }
                });

            return result;
        }

        private IEnumerable<T> ExecuteQuery(TransactionCache transactionCache)
        {
            var takeCount = _takeCount ?? int.MaxValue;

            foreach (var block in ListBlocks(transactionCache))
            {
                var recordIds = block.Query(_predicate);
                var records = block.GetRecords(recordIds)
                    .Cast<T>();

                foreach (var record in records)
                {
                    yield return record;
                    --takeCount;
                    if (takeCount == 0)
                    {
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<IBlock> ListBlocks(TransactionCache transactionCache)
        {
            if (transactionCache
                .UncommittedTransactionLog
                .TableTransactionLogMap.TryGetValue(_table.Schema.TableName, out var ul))
            {
                yield return ul.BlockBuilder;
            }
            foreach (var committedLog in transactionCache.DatabaseCache.CommittedLogs)
            {
                if (committedLog
                    .TableTransactionLogs
                    .TryGetValue(_table.Schema.TableName, out var cl))
                {
                    yield return cl.InMemoryBlock;
                }
            }
        }
        #endregion
    }
}
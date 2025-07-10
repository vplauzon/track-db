using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
        where T : notnull
    {
        private readonly Database _database;
        private readonly TableSchema<T> _schema;

        internal Table(Database database, TableSchema<T> schema)
        {
            _database = database;
            _schema = schema;
        }

        #region Append
        public void AppendRecord(T record, TransactionContext? transactionContext = null)
        {
            _database.ExecuteWithinTransactionContext(
                transactionContext,
                transactionCache =>
                {
                    AppendRecordInternal(record, transactionCache);
                });
        }

        public void AppendRecords(
            IEnumerable<T> records,
            TransactionContext? transactionContext = null)
        {
            _database.ExecuteWithinTransactionContext(
                transactionContext,
                transactionCache =>
                {
                    foreach (var record in records)
                    {
                        AppendRecordInternal(record, transactionCache);
                    }
                });
        }

        private void AppendRecordInternal(T record, TransactionCache transactionCache)
        {
            transactionCache.TransactionLog.AppendRecord(_database.NewRecordId(), record, _schema);
        }
        #endregion

        #region Query
        public IImmutableList<T> Query(
            Expression<Func<T, bool>> predicateExpression,
            TransactionContext? transactionContext = null)
        {
            var queryPredicate = QueryPredicateFactory.Create(predicateExpression);
            var result =_database.ExecuteWithinTransactionContext(
                transactionContext,
                transactionCache =>
                {
                    return QueryInternal(queryPredicate, transactionCache);
                });

            return result;
        }

        private IImmutableList<T> QueryInternal(
            IQueryPredicate<T> queryPredicate,
            TransactionCache transactionCache)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
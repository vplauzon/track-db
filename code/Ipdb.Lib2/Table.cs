using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
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
        internal Table(Database database, TableSchema<T> schema)
        {
            Database = database;
            Schema = schema;
        }

        public Database Database { get; }

        public TableSchema<T> Schema { get; }

        #region Append
        public void AppendRecord(T record, TransactionContext? transactionContext = null)
        {
            Database.ExecuteWithinTransactionContext(
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
            Database.ExecuteWithinTransactionContext(
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
            transactionCache.TransactionLog.AppendRecord(Database.NewRecordId(), record, Schema);
        }
        #endregion

        #region Query
        public TableQuery<T> Query(TransactionContext? transactionContext = null)
        {
            return new TableQuery<T>(this, transactionContext);
        }
        #endregion
    }
}
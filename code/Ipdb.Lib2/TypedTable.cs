using Ipdb.Lib2.Cache;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class TypedTable<T> : Table
        where T : notnull
    {
        internal TypedTable(Database database, TypedTableSchema<T> schema)
            : base(database, schema)
        {
        }

        public new TypedTableSchema<T> Schema => (TypedTableSchema<T>)base.Schema;

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
            var columns = Schema.FromObjectToColumns(record);

            transactionCache.UncommittedTransactionLog.AppendRecord(
                Database.NewRecordId(),
                columns,
                Schema);
        }
        #endregion

        #region Query
        public TypedTableQuery<T> Query(TransactionContext? transactionContext = null)
        {
            return new TypedTableQuery<T>(this, transactionContext);
        }
        #endregion
    }
}
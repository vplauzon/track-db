using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class TypedTable<T> : Table
        where T : notnull
    {
        internal TypedTable(Database database, TypedTableSchema<T> schema)
            : base(database, schema)
        {
            PredicateFactory = new QueryPredicateFactory<T>(Schema);
        }

        internal QueryPredicateFactory<T> PredicateFactory { get; }

        public new TypedTableSchema<T> Schema => (TypedTableSchema<T>)base.Schema;

        #region Append
        public void AppendRecord(T record, TransactionContext? transactionContext = null)
        {
            var columns = Schema.FromObjectToColumns(record);

            AppendRecord(columns, transactionContext);
        }

        public void AppendRecords(
            IEnumerable<T> records,
            TransactionContext? transactionContext = null)
        {
            Database.ExecuteWithinTransactionContext(
                transactionContext,
                tc =>
                {
                    foreach (var record in records)
                    {
                        AppendRecordInternal(record, tc);
                    }
                });
        }

        private void AppendRecordInternal(T record, TransactionContext? transactionContext)
        {
            var columns = Schema.FromObjectToColumns(record);

            AppendRecord(columns, transactionContext);
        }
        #endregion

        #region Query
        public TypedTableQuery<T> Query(TransactionContext? transactionContext = null)
        {
            return new TypedTableQuery<T>(this, transactionContext);
        }
        #endregion

        #region Query

        internal void UpdateRecord(
            T oldRecordVersion,
            T newRecordVersion,
            TransactionContext? transactionContext = null)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
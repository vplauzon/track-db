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
        public new TypedTableQuery<T> Query(TransactionContext? tx = null)
        {
            return new TypedTableQuery<T>(this, true, tx);
        }

        public new IEnumerable<T> TombstonedWithinTransaction(TransactionContext tx)
        {
            var records = base.TombstonedWithinTransaction(tx);
            var typedRecords = records
                .Select(r => Schema.FromColumnsToObject(r.Span));

            return typedRecords;
        }
        #endregion

        #region Update
        public int UpdateRecord(
            T oldRecordVersion,
            T newRecordVersion,
            TransactionContext? tx = null)
        {
            var oldColumns = Schema.FromObjectToColumns(oldRecordVersion);
            var newColumns = Schema.FromObjectToColumns(newRecordVersion);

            return UpdateRecord(oldColumns, newColumns, tx);
        }
        #endregion
    }
}
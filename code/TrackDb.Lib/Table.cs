using TrackDb.Lib.InMemory;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace TrackDb.Lib
{
    public class Table
    {
        internal Table(Database database, TableSchema schema)
        {
            Database = database;
            Schema = schema;
        }

        public Database Database { get; }

        public TableSchema Schema { get; }

        #region Append
        public void AppendRecord(
            ReadOnlySpan<object?> record,
            TransactionContext? transactionContext = null)
        {
            if (record.Length != Schema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    $"Expected '{Schema.Columns.Count}' but has '{record.Length}'",
                    nameof(record));
            }
            //  Database.ExecuteWithinTransactionContext doesn't work with ReadOnlySpan<object?>
            if (transactionContext != null)
            {
                AppendRecordInternal(record, transactionContext);
            }
            else
            {
                using (transactionContext = Database.CreateTransaction())
                {
                    AppendRecordInternal(record, transactionContext);

                    transactionContext.Complete();
                }
            }
        }

        public void AppendRecords(
            IEnumerable<ReadOnlySpan<object?>> records,
            TransactionContext? transactionContext = null)
        {
            Database.ExecuteWithinTransactionContext(
                transactionContext,
                tc =>
                {
                    foreach (var record in records)
                    {
                        AppendRecord(record, tc);
                    }
                });
        }

        private void AppendRecordInternal(
            ReadOnlySpan<object?> record,
            TransactionContext transactionContext)
        {
            transactionContext.TransactionState.UncommittedTransactionLog.AppendRecord(
                Database.NewRecordId(),
                record,
                Schema);
        }
        #endregion

        #region Update
        public void UpdateRecord(
            ReadOnlySpan<object?> oldVersionRecord,
            ReadOnlySpan<object?> newVersionRecord,
            TransactionContext? tx = null)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
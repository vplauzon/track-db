using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TrackDb.Lib.Predicate;

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

        public TableQuery Query(TransactionContext? transactionContext = null)
        {
            return new TableQuery(this, true, transactionContext);
        }

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
        public int UpdateRecord(
            ReadOnlySpan<object?> oldVersionRecord,
            ReadOnlySpan<object?> newVersionRecord,
            TransactionContext? tx = null)
        {
            if (oldVersionRecord.Length != Schema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    $"Expected '{Schema.Columns.Count}' but has '{oldVersionRecord.Length}'",
                    nameof(oldVersionRecord));
            }
            if (newVersionRecord.Length != Schema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    $"Expected '{Schema.Columns.Count}' but has '{newVersionRecord.Length}'",
                    nameof(newVersionRecord));
            }
            if (tx != null)
            {
                return UpdateRecordInternal(oldVersionRecord, newVersionRecord, tx);
            }
            else
            {
                using (tx = Database.CreateTransaction())
                {
                    var deletedCount =
                        UpdateRecordInternal(oldVersionRecord, newVersionRecord, tx);

                    tx.Complete();

                    return deletedCount;
                }
            }
        }

        private int UpdateRecordInternal(
            ReadOnlySpan<object?> oldVersionRecord,
            ReadOnlySpan<object?> newVersionRecord,
            TransactionContext? tx)
        {
            var deletedRecordCount = Query(tx)
                .WithPredicate(CreatePrimaryKeyEqualPredicate(oldVersionRecord))
                .Delete();

            AppendRecord(newVersionRecord, tx);

            return deletedRecordCount;
        }

        private QueryPredicate CreatePrimaryKeyEqualPredicate(
            ReadOnlySpan<object?> oldVersionRecord)
        {
            QueryPredicate? predicate = null;

            foreach (var columnIndex in Schema.PrimaryKeyColumnIndexes)
            {
                var value = oldVersionRecord[columnIndex];
                var newPredicate = new BinaryOperatorPredicate(
                    columnIndex,
                    value,
                    BinaryOperator.Equal);

                predicate = predicate == null
                    ? newPredicate
                    : new ConjunctionPredicate(predicate, newPredicate);
            }

            if (predicate == null)
            {
                throw new InvalidOperationException(
                    $"No primary defined on table '{Schema.TableName}':  " +
                    $"can't participate in an update");
            }

            return predicate!;
        }
        #endregion
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib
{
    public class Table
    {
        private long _recordId = 0;

        internal Table(Database database, TableSchema schema)
        {
            Database = database;
            Schema = schema;
        }

        public Database Database { get; }

        public TableSchema Schema { get; }

        public TableQuery Query(TransactionContext? tx = null)
        {
            return new TableQuery(this, tx, true);
        }

        public IEnumerable<ReadOnlyMemory<object?>> TombstonedWithinTransaction(
            TransactionContext tx)
        {
            var tombstoneRecordIds = Database.TombstoneTable.Query(tx)
                .WithinTransactionOnly()
                .Where(pf => pf.Equal(t => t.TableName, Schema.TableName))
                .TableQuery
                .WithProjection(Database.TombstoneTable.Schema.GetColumnIndexSubset(
                    t => t.DeletedRecordId))
                .Select(r => r.Span[0])
                .ToImmutableArray();
            var tombstonedRecords = Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(new InPredicate(
                    Schema.RecordIdColumnIndex,
                    tombstoneRecordIds,
                    true));

            return tombstonedRecords;
        }

        internal void InitRecordId(long maxRecordId)
        {
            _recordId = maxRecordId;
        }

        #region Record IDs
        internal long NewRecordId()
        {
            return Interlocked.Increment(ref _recordId);
        }

        internal IImmutableList<long> NewRecordIds(int recordCount)
        {
            var nextId = Interlocked.Add(ref _recordId, recordCount);

            return Enumerable.Range(0, recordCount)
                .Select(i => i + nextId - recordCount)
                .ToImmutableArray();
        }
        #endregion

        #region Append
        public void AppendRecord(ReadOnlySpan<object?> record, TransactionContext? tx = null)
        {
            //  Database.ExecuteWithinTransactionContext doesn't work with ReadOnlySpan<object?>
            if (tx != null)
            {
                AppendRecordInternal(record, tx);
            }
            else
            {
                using (tx = Database.CreateTransaction())
                {
                    AppendRecordInternal(record, tx);

                    tx.Complete();
                }
            }
        }

        public void AppendRecords(
            IEnumerable<ReadOnlySpan<object?>> records,
            TransactionContext? tx = null)
        {
            Database.ExecuteWithinTransactionContext(
                tx,
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
            TransactionContext tx)
        {
            if (record.Length != Schema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    $"Expected '{Schema.Columns.Count}' but has '{record.Length}'",
                    nameof(record));
            }
            tx.TransactionState.UncommittedTransactionLog.AppendRecord(
                NewRecordId(),
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
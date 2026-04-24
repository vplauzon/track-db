using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib
{
    public class TransactionContext : IDisposable
    {
        #region Inner types
        private enum TransactionStatus
        {
            Open,
            Complete,
            Cancelled
        }
        #endregion

        private readonly Database _database;
        private TransactionStatus _status = TransactionStatus.Open;

        #region Constructors
        /// <summary>Creates a transaction.</summary>
        /// <param name="database"></param>
        /// <param name="inMemoryDatabase"></param>
        /// <param name="transactionLog"></param>
        /// <param name="doLog"></param>
        internal TransactionContext(
            Database database,
            InMemoryDatabase inMemoryDatabase,
            TransactionLog transactionLog,
            bool doLog)
        {
            _database = database;
            TransactionState = new TransactionState(inMemoryDatabase, transactionLog);
            DoLog = doLog;
        }
        #endregion

        void IDisposable.Dispose()
        {
            if (_status == TransactionStatus.Open)
            {
                Rollback();
            }
        }

        internal bool DoLog { get; }

        internal TransactionState TransactionState { get; }

        internal bool HasUserTableData
        {
            get
            {
                var tableNames =
                    TransactionState.UncommittedTransactionLog.TransactionTableLogMap.Keys;
                var tombstoneName = _database.TombstoneTable.Schema.TableName;
                var tableMap = _database.GetDatabaseStateSnapshot().TableMap;
                var userTableNames = tableNames
                    .Where(t => tableMap[t].IsUserTable || t == tombstoneName);

                return userTableNames.Any();
            }
        }

        public void Complete()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                _database.CompleteTransaction(this);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_status}'");
            }
        }

        /// <summary>
        /// Guarantee the transaction (and everyone before) is flushed to logs upon return.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public async Task CompleteAsync(CancellationToken ct = default)
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                await _database.CompleteTransactionAsync(this, ct);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_status}'");
            }
        }

        public void Rollback()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Cancelled;
                _database.RollbackTransaction();
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_status}'");
            }
        }

        /// <summary>
        /// Load all committed transaction logs of a table, merges them, hard delete previously
        /// deleted records and stores it in <see cref="TransactionTableLog.ReplacingDataBlock"/>.
        /// If the table is already loaded, nothing happens.
        /// The tombstone table might get loaded as a side effect.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns><c>true</c> iif something was loaded.</returns>
        internal bool LoadCommittedBlocksInTransaction(string tableName)
        {
            var schema = _database.GetAnyTable(tableName).Schema;
            var uncommittedMap =
                TransactionState.UncommittedTransactionLog.TransactionTableLogMap;

            if (TransactionState.LoadCommittedBlocksInTransaction(tableName))
            {
                if (!schema.IsMetadata
                    && tableName != _database.TombstoneTable.Schema.TableName)
                {
                    HardDeleteCommittedRecords(tableName, (DataTableSchema)schema);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>Load all block tombstones in the transaction.</summary>
        internal void LoadBlockTombstonesInTransaction()
        {
            var log = TransactionState.UncommittedTransactionLog;

            if (log.ReplacingBlockTombstonesIndex == null)
            {
                log.ReplacingBlockTombstonesIndex =
                    TransactionState.InMemoryDatabase.BlockTombstonesIndex.ToDictionary();
            }
        }

        /// <summary>Load all <see cref="AvailableBlock"/> in the transaction.</summary>
        internal void LoadAvailableBlockInTransaction()
        {
            var log = TransactionState.UncommittedTransactionLog;

            if (log.ReplacingAvailableBlockIndex == null)
            {
                var availableBlockIndex = TransactionState.InMemoryDatabase.AvailableBlockIndex;

                log.ReplacingAvailableBlockIndex = availableBlockIndex
                    .ToDictionary(p => p.Key, p => p.Value.ToDictionary());
            }
        }

        internal void CleanTable(TableSchema schema)
        {
            TransactionState.CleanTable(schema);
        }

        private void HardDeleteCommittedRecords(string tableName, DataTableSchema schema)
        {   //  We hard delete only out-of-transaction tombstones
            var deletedRecordIdColumnSet =
                _database.TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId);
            var deletedRecordIds = _database.TombstoneTable.Query(this)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .TableQuery
                .WithProjection(deletedRecordIdColumnSet)
                .Select(t => (long)t.Span[0]!);
            var recordIdPredicate =
                new InPredicate<long>(schema.RecordIdColumnIndex, deletedRecordIds, true);

            if (recordIdPredicate.Values.Count > 0)
            {
                var committedDataBlockBuilder = TransactionState.UncommittedTransactionLog
                    .TransactionTableLogMap[tableName]
                    .ReplacingDataBlock!;
                IBlock committedDataBlock = committedDataBlockBuilder;
                //  Hard delete in-memory records in the table
                var rowIndexes = committedDataBlock.Filter(recordIdPredicate, false).RowIndexes;

                if (rowIndexes.Count > 0)
                {
                    var hardDeletedRecordIds = committedDataBlock
                        .Project(new object?[1], [schema.RecordIdColumnIndex], rowIndexes)
                        .Select(r => (long)r.Span[0]!)
                        .ToArray();

                    committedDataBlockBuilder.DeleteRecordsByRecordIndex(rowIndexes);
                    _database.DeleteTombstoneRecords(tableName, hardDeletedRecordIds, this);
                }
            }
        }
    }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;

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
        private readonly bool _doLog;
        private TransactionStatus _status = TransactionStatus.Open;

        #region Constructors
        /// <summary>Creates a transaction.</summary>
        /// <param name="database"></param>
        /// <param name="inMemoryDatabase"></param>
        /// <param name="doLog"></param>
        internal TransactionContext(
            Database database,
            InMemoryDatabase inMemoryDatabase,
            bool doLog)
        {
            _database = database;
            TransactionState = new TransactionState(inMemoryDatabase, new TransactionLog());
            _doLog = doLog;
        }
        #endregion

        void IDisposable.Dispose()
        {
            if (_status == TransactionStatus.Open)
            {
                Rollback();
            }
        }

        internal TransactionState TransactionState { get; }

        public void Complete()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                _database.CompleteTransaction(TransactionState, _doLog);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_status}'");
            }
        }

        public async Task LogAndCompleteAsync()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                await _database.LogAndCompleteTransactionAsync(TransactionState, _doLog);
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
        /// deleted records and stores it in <see cref="TransactionTableLog.CommittedDataBlock"/>.
        /// If the table is already loaded, nothing happens.
        /// The tombstone table might get loaded as a side effect.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns><c>true</c> iif something was loaded.</returns>
        internal bool LoadCommittedBlocksInTransaction(string tableName)
        {
            var transactionState = TransactionState;

            if (transactionState.LoadCommittedBlocksInTransaction(tableName))
            {
                if (tableName != _database.TombstoneTable.Schema.TableName)
                {
                    var deletedRecordIds = _database.GetDeletedRecordIds(tableName, this)
                        .ToImmutableArray();

                    if (deletedRecordIds.Any())
                    {
                        var tombstoneTableName = _database.TombstoneTable.Schema.TableName;

                        transactionState.LoadCommittedBlocksInTransaction(tombstoneTableName);

                        var tableBlockBuilder = transactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[tableName]
                            .CommittedDataBlock!;
                        var tombstoneBlockBuilder = transactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[tombstoneTableName]
                            .CommittedDataBlock!;
                        //  Hard delete in-memory records in the table
                        var hardDeletedRecordIds = tableBlockBuilder.DeleteRecordsByRecordId(
                            deletedRecordIds);

                        if (hardDeletedRecordIds.Any())
                        {   //  Hard delete tombstone records in the tombstone table
                            var rowIndexes = _database.TombstoneTable.Query(this)
                                .Where(pf => pf.Equal(t => t.TableName, tableName))
                                .Where(pf => pf.In(t => t.DeletedRecordId, hardDeletedRecordIds))
                                .TableQuery
                                .WithProjection([_database.TombstoneTable.Schema.RowIndexColumnIndex])
                                .Select(r => (int)r.Span[0]!);

                            tombstoneBlockBuilder.DeleteRecordsByRecordIndex(rowIndexes);
                        }
                    }
                }

                return true;
            }

            return false;
        }
    }
}
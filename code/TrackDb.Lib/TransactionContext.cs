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
                {   //  We hard delete only out-of-transaction tombstones
                    var deletedRecordIds = _database.TombstoneTable.Query(this)
                        .WithCommittedOnly()
                        .Where(pf => pf.Equal(t => t.TableName, tableName))
                        .Select(t => t.DeletedRecordId)
                        .ToImmutableArray();

                    if (deletedRecordIds.Any())
                    {
                        var committedDataBlock = transactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[tableName]
                            .CommittedDataBlock!;
                        //  Hard delete in-memory records in the table
                        var hardDeletedRecordIds = committedDataBlock.DeleteRecordsByRecordId(
                            deletedRecordIds);

                        if (hardDeletedRecordIds.Any())
                        {
                            _database.DeleteTombstoneRecords(
                                tableName,
                                hardDeletedRecordIds,
                                this);
                        }
                    }
                }

                return true;
            }

            return false;
        }
    }
}
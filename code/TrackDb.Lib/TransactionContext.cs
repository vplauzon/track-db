using System;
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

        private static long _nextTransactionId = 0;

        private readonly Database _database;
        private readonly Func<long, TransactionState> _stateResolutionFunc;
        private readonly bool _doLog;
        private TransactionStatus _status = TransactionStatus.Open;

        #region Constructors
        /// <summary>Creates a transaction.</summary>
        /// <param name="database"></param>
        /// <param name="stateResolutionFunc"></param>
        internal TransactionContext(
            Database database,
            Func<long, TransactionState> stateResolutionFunc,
            bool doLog)
            : this(
                  database,
                  stateResolutionFunc,
                  doLog,
                  Interlocked.Increment(ref _nextTransactionId))
        {
        }

        /// <summary>
        /// Creates a dummy transaction that will not commit.
        /// </summary>
        /// <remarks>This is more lightweight to use.</remarks>
        /// <param name="database"></param>
        /// <param name="transactionState"></param>
        internal TransactionContext(
            Database database,
            TransactionState transactionState)
            : this(
                  database,
                  txId => transactionState,
                  false,
                  0)
        {
        }

        private TransactionContext(
            Database database,
            Func<long, TransactionState> stateResolutionFunc,
            bool doLog,
            long transactionId)
        {
            _database = database;
            _stateResolutionFunc = stateResolutionFunc;
            _doLog = doLog;
            TransactionId = transactionId;
        }
        #endregion

        void IDisposable.Dispose()
        {
            if (_status == TransactionStatus.Open)
            {
                Rollback();
            }
        }

        public long TransactionId { get; }

        internal TransactionState TransactionState => _stateResolutionFunc(TransactionId);

        public void Complete()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                if (TransactionId != 0)
                {
                    _database.CompleteTransaction(TransactionId, _doLog);
                }
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
                if (TransactionId != 0)
                {
                    await _database.LogAndCompleteTransactionAsync(TransactionId, _doLog);
                }
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
                if (TransactionId != 0)
                {
                    _database.RollbackTransaction(TransactionId);
                }
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
            throw new NotImplementedException();
        }
    }
}
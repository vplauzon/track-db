using TrackDb.Lib.InMemory;
using System;
using System.Threading;
using System.Threading.Tasks;

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
        private TransactionStatus _status = TransactionStatus.Open;

        #region Constructors
        /// <summary>Creates a transaction.</summary>
        /// <param name="database"></param>
        /// <param name="stateResolutionFunc"></param>
        internal TransactionContext(
            Database database,
            Func<long, TransactionState> stateResolutionFunc)
            : this(
                  database,
                  stateResolutionFunc,
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
                  0)
        {
        }

        private TransactionContext(
            Database database,
            Func<long, TransactionState> stateResolutionFunc,
            long transactionId)
        {
            _database = database;
            _stateResolutionFunc = stateResolutionFunc;
            TransactionId = transactionId;
        }
        #endregion

        public long TransactionId { get; }

        internal TransactionState TransactionState => _stateResolutionFunc(TransactionId);

        void IDisposable.Dispose()
        {
            if (_status == TransactionStatus.Open)
            {
                Rollback();
            }
        }

        public void Complete()
        {
            if (_status == TransactionStatus.Open)
            {
                _status = TransactionStatus.Complete;
                if (TransactionId != 0)
                {
                    _database.CompleteTransaction(TransactionId);
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
            await Task.CompletedTask;
            Complete();
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
    }
}
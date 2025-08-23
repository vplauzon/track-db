using TrackDb.Lib.Cache;
using System;
using System.Threading;

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
        private readonly Func<long, TransactionState> _cacheResolutionFunc;
        private TransactionStatus _state = TransactionStatus.Open;

        #region Constructors
        /// <summary>Creates a transaction.</summary>
        /// <param name="database"></param>
        /// <param name="cacheResolutionFunc"></param>
        internal TransactionContext(
            Database database,
            Func<long, TransactionState> cacheResolutionFunc)
            : this(
                  database,
                  cacheResolutionFunc,
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
            Func<long, TransactionState> cacheResolutionFunc,
            long transactionId)
        {
            _database = database;
            _cacheResolutionFunc = cacheResolutionFunc;
            TransactionId = transactionId;
        }
        #endregion

        public long TransactionId { get; }

        internal TransactionState TransactionState => _cacheResolutionFunc(TransactionId);

        void IDisposable.Dispose()
        {
            if (_state == TransactionStatus.Open)
            {
                Rollback();
            }
        }

        public void Complete()
        {
            if (_state == TransactionStatus.Open)
            {
                _state = TransactionStatus.Complete;
                if (TransactionId != 0)
                {
                    _database.CompleteTransaction(TransactionId);
                }
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_state}'");
            }
        }

        public void Rollback()
        {
            if (_state == TransactionStatus.Open)
            {
                _state = TransactionStatus.Cancelled;
                if (TransactionId != 0)
                {
                    _database.RollbackTransaction(TransactionId);
                }
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_state}'");
            }
        }
    }
}
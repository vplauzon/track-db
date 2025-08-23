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

        internal TransactionContext(
            Database database,
            Func<long, TransactionState> cacheResolutionFunc)
        {
            _database = database;
            _cacheResolutionFunc = cacheResolutionFunc;
            TransactionId = Interlocked.Increment(ref _nextTransactionId);
        }

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
                _database.CompleteTransaction(TransactionId);
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
                _database.RollbackTransaction(TransactionId);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_state}'");
            }
        }
    }
}
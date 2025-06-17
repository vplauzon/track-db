using System;
using System.Threading;

namespace Ipdb.Lib
{
    public class TransactionContext : IDisposable
    {
        #region Inner types
        private enum TransactionState
        {
            Open,
            Complete,
            Cancelled
        }
        #endregion

        private static long _nextTransactionId = 0;

        private readonly Database _database;
        private TransactionState _state = TransactionState.Open;

        internal TransactionContext(Database database)
        {
            TransactionId = Interlocked.Increment(ref _nextTransactionId);
            _database = database;
        }

        public long TransactionId { get; }

        void IDisposable.Dispose()
        {
            if (_state == TransactionState.Open)
            {
                Rollback();
            }
        }

        public void Complete()
        {
            if (_state == TransactionState.Open)
            {
                _state = TransactionState.Complete;
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
            if (_state == TransactionState.Open)
            {
                _state = TransactionState.Cancelled;
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
using System;

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

        private readonly DataManager _storageManager;
        private TransactionState _state = TransactionState.Open;

        internal TransactionContext(long transactionId, DataManager storageManager)
        {
            TransactionId = transactionId;
            _storageManager = storageManager;
            storageManager.OpenTransaction(TransactionId);
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
                _storageManager.CompleteTransaction(TransactionId);
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
                _storageManager.RollbackTransaction(TransactionId);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_state}'");
            }
        }
    }
}
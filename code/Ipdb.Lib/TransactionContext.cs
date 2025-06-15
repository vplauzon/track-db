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

        private readonly DataManager _dataManager;
        private TransactionState _state = TransactionState.Open;

        internal TransactionContext(long transactionId, DataManager dataManager)
        {
            TransactionId = transactionId;
            _dataManager = dataManager;
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
                _dataManager.CompleteTransaction(TransactionId);
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
                _dataManager.RollbackTransaction(TransactionId);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Transaction context is in terminal state of '{_state}'");
            }
        }
    }
}
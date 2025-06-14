using System;

namespace Ipdb.Lib
{
    public class TransactionContext : IDisposable
    {
        internal TransactionContext(long transactionId)
        {
            TransactionId = transactionId;
        }

        public long TransactionId { get; }

        void IDisposable.Dispose()
        {
        }

        public void Complete()
        {
        }

        public void Cancel()
        {
        }
    }
}
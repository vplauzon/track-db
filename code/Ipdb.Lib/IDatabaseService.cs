using Ipdb.Lib.Cache;

namespace Ipdb.Lib
{
    internal interface IDatabaseService
    {
        long GetNewDocumentRevisionId();

        void ObserveBackgroundTasks();

        TransactionCache GetTransactionCache(long transactionId);

        TransactionContext CreateTransaction();
    }
}
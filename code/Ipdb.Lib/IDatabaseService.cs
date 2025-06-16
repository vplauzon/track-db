using Ipdb.Lib.Cache;

namespace Ipdb.Lib
{
    internal interface IDatabaseService
    {
        long GetNewDocumentRevisionId();

        TransactionCache GetTransactionCache(long transactionId);

        TransactionContext CreateTransaction();
    }
}
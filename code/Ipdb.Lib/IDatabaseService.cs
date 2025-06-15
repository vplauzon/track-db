using Ipdb.Lib.Cache;

namespace Ipdb.Lib
{
    internal interface IDatabaseService
    {
        TransactionCache GetTransactionCache(long transactionId);
    }
}
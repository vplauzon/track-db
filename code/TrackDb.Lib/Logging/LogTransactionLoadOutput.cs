using System.Collections.Generic;
using TrackDb.Lib.InMemory;

namespace TrackDb.Lib.Logging
{
    internal record LogTransactionLoadOutput(
        bool IsCheckpointRequired,
        IAsyncEnumerable<TransactionLog> Transactions);
}
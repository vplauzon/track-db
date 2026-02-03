using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;

namespace TrackDb.Lib
{
    /// <summary>Transaction log item.</summary>
    /// <param name="TransactionLog">Single transaction log.</param>
    /// <param name="TransactionLogsFunc">Listing all transaction logs for a checkpoint.</param>
    /// <param name="Tcs">Optional continuation.</param>
    /// <param name="Ct"></param>
    internal record TransactionLogItem(
        TransactionLog? TransactionLog,
        Func<IEnumerable<TransactionLog>>? TransactionLogsFunc,
        TaskCompletionSource? Tcs,
        CancellationToken Ct);
}
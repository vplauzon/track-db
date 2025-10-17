using System.Collections.Generic;

namespace TrackDb.Lib.Logging
{
    internal record LogStorageLoadOutput(
        bool IsCheckpointRequired,
        IAsyncEnumerable<string> TransactionTexts);
}
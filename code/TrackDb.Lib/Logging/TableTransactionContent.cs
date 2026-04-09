using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace TrackDb.Lib.Logging
{
    internal record TableTransactionContent(
        NewRecordsContent? NewRecordsContent,
        IImmutableList<long>? TombstoneRecordIds)
    {
        public int GetRowCount()
        {
            return (NewRecordsContent?.NewRecordIds.Count ?? 0)
                + (TombstoneRecordIds?.Count ?? 0);
        }
    }
}
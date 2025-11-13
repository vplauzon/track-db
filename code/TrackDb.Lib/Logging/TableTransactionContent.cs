using System.Collections.Generic;
using System.Collections.Immutable;

namespace TrackDb.Lib.Logging
{
    internal record TableTransactionContent(
		NewRecordsContent? NewRecordsContent,
		IImmutableList<long>? TombstoneRecordIds);
}
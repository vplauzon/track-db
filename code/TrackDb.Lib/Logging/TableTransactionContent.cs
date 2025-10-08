using System.Collections.Generic;

namespace TrackDb.Lib.Logging
{
    internal record TableTransactionContent(
        string TableName,
        List<long> TombstoneRecordIds,
        List<ColumnTransactionContent> Columns);
}
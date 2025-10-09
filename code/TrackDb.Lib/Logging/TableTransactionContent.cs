using System.Collections.Generic;

namespace TrackDb.Lib.Logging
{
    internal record TableTransactionContent(
        string TableName,
        List<long> NewRecordIds,
        List<ColumnTransactionContent> Columns);
}
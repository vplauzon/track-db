using System.Collections.Generic;
using System.Text.Json;

namespace TrackDb.Lib.Logging
{
    internal record ColumnTransactionContent(
        string ColumnName,
        List<JsonElement> Elements);
}
using System.Collections.Generic;
using System.Text.Json;

namespace TrackDb.Lib.Logging
{
    internal record TableTransactionContent(
        List<long> NewRecordIds,
        Dictionary<string, List<JsonElement>> Columns);
}
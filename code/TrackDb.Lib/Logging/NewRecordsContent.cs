using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.Json;

namespace TrackDb.Lib.Logging
{
    internal record NewRecordsContent(
        IImmutableList<long> NewRecordIds,
        IImmutableDictionary<string, List<JsonElement>> Columns);
}
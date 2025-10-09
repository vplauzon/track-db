using System.Collections.Generic;

namespace TrackDb.Lib.Logging
{
    internal record TombstoneContent(string TableName, List<long> TombstoneRecordIds);
}
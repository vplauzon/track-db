using System;

namespace TrackDb.Lib
{
    internal record TombstoneRecord(
        long RecordId,
        int? BlockId,
        string TableName,
        DateTime Timestamp);
}
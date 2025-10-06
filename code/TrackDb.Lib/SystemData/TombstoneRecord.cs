using System;

namespace TrackDb.Lib.SystemData
{
    internal record TombstoneRecord(
        long RecordId,
        int? BlockId,
        string TableName,
        DateTime Timestamp);
}
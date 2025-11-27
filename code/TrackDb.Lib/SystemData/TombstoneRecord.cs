using System;

namespace TrackDb.Lib.SystemData
{
    internal record TombstoneRecord(
        long DeletedRecordId,
        string TableName,
        int? BlockId,
        DateTime Timestamp);
}
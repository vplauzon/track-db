using System;

namespace TrackDb.Lib.SystemData
{
    internal record TombstoneRecord(
        long DeletedRecordId,
        int? BlockId,
        string TableName,
        DateTime Timestamp);
}
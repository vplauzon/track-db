using System;

namespace TrackDb.Lib.SystemData
{
    internal record TombstoneRecord(
        long DeletedRecordId,
        string TableName,
        //  This is the block-ID of the record at the time of deletion
        //  It is to be used as a hint and should not be relied on
        //  Due to the asynchronous behaviour of garbage collection, a record can be deleted at
        //  the same time (in another transaction) its block is compacted / merged.
        int? BlockId,
        DateTime Timestamp);
}
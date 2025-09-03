namespace TrackDb.Lib
{
    internal record TombstoneRecord(long RecordId, long? BlockId, string TableName);
}
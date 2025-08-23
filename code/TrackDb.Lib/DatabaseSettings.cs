namespace TrackDb.Lib
{
    public record DatabaseSettings(
        int MaxCachedRecordsPerDb = 100,
        int MaxMetaDataCachedRecordsPerTable = 50);
}
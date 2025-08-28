namespace TrackDb.Lib.Settings
{
    public record DatabaseSettings(
        int MaxCachedRecordsPerDb = 100,
        int MaxMetaDataCachedRecordsPerTable = 50,
        int MaxInMemoryBlocksPerTable = 5);
}
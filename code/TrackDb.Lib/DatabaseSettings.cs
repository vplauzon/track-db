namespace Ipdb.Lib2
{
    public record DatabaseSettings(
        int MaxCachedRecordsPerDb = 100,
        int MaxMetaDataCachedRecordsPerTable = 50);
}
using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MaxUserDataRecords,
        int MaxMetaDataRecords,
        int MaxTombstonedRecords,
        TimeSpan MaxTombstonePeriod)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MaxUserDataRecords = null,
            int? MaxMetaDataRecords = null,
            int? MaxTombstonedRecords = null,
            TimeSpan? MaxTombstonePeriod = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 5,
                 MaxUserDataRecords ?? 100,
                 MaxMetaDataRecords ?? 100,
                 MaxTombstonedRecords ?? 100,
                 MaxTombstonePeriod ?? TimeSpan.FromMinutes(2));
        }
    }
}
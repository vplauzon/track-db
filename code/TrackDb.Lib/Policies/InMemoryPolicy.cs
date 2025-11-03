using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MaxNonMetaDataRecords,
        int MaxMetaDataRecords,
        int MaxTombstonedRecords,
        TimeSpan MaxTombstonePeriod)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MaxNonMetaDataRecords = null,
            int? MaxMetaDataRecords = null,
            int? MaxTombstonedRecords = null,
            TimeSpan? MaxTombstonePeriod = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 10,
                 MaxNonMetaDataRecords ?? 100,
                 MaxMetaDataRecords ?? 100,
                 MaxTombstonedRecords ?? 100,
                 MaxTombstonePeriod ?? TimeSpan.FromMinutes(2));
        }
    }
}
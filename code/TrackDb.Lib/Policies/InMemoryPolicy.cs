using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MaxNonMetaDataRecords,
        int MaxMetaDataRecords,
        TimeSpan MaxPersistancePeriod,
        int MaxTombstonedRecords,
        TimeSpan MaxTombstonePeriod)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MaxNonMetaDataRecords = null,
            int? MaxMetaDataRecords = null,
            TimeSpan? MaxPersistancePeriod = null,
            int? MaxTombstonedRecords = null,
            TimeSpan? MaxTombstonePeriod = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 10,
                 MaxNonMetaDataRecords ?? 500,
                 MaxMetaDataRecords ?? 50,
                 MaxPersistancePeriod ?? TimeSpan.FromSeconds(60),
                 MaxTombstonedRecords ?? 600,
                 MaxTombstonePeriod ?? TimeSpan.FromSeconds(15));
        }
    }
}
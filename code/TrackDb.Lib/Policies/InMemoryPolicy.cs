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
                 MaxNonMetaDataRecords ?? 200,
                 MaxMetaDataRecords ?? 50,
                 MaxPersistancePeriod ?? TimeSpan.FromSeconds(30),
                 MaxTombstonedRecords ?? 200,
                 MaxTombstonePeriod ?? TimeSpan.FromSeconds(10));
        }
    }
}
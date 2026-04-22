using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MaxNonMetaDataRecords,
        int MaxMetaDataRecords,
        TimeSpan MaxPersistancePeriod)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MaxNonMetaDataRecords = null,
            int? MaxMetaDataRecords = null,
            TimeSpan? MaxPersistancePeriod = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 10,
                 MaxNonMetaDataRecords ?? 500,
                 MaxMetaDataRecords ?? 50,
                 MaxPersistancePeriod ?? TimeSpan.FromSeconds(60));
        }
    }
}
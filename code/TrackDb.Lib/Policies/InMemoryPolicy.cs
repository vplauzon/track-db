using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MinNonMetaDataRecordsPerBlock,
        int MaxNonMetaDataRecordsInMemory,
        int MinMetaDataRecordsPerBlock,
        int MaxMetaDataRecordsInMemory,
        int MaxTombstonedRecords,
        TimeSpan MaxTombstonePeriod)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MinNonMetaDataRecordsPerBlock = null,
            int? MaxNonMetaDataRecordsInMemory = null,
            int? MinMetaDataRecordsPerBlock = null,
            int? MaxMetaDataRecordsInMemory = null,
            int? MaxTombstonedRecords = null,
            TimeSpan? MaxTombstonePeriod = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 5,
                 MinNonMetaDataRecordsPerBlock ?? 20,
                 MaxNonMetaDataRecordsInMemory ?? 100,
                 MinMetaDataRecordsPerBlock ?? 20,
                 MaxMetaDataRecordsInMemory ?? 100,
                 MaxTombstonedRecords ?? 100,
                 MaxTombstonePeriod ?? TimeSpan.FromMinutes(2));
        }
    }
}
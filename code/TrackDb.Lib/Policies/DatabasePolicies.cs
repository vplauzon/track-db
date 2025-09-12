using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicies(
        int MaxInMemoryBlocksPerTable = 5,
        int MaxInMemoryUserDataRecords = 100,
        int MaxInMemoryMetaDataRecords = 100,
        int MaxTombstonedRecords = 100,
        TimeSpan? MaxTombstonePeriod = null)
    {
        public static TimeSpan DefaultMaxTombstonePeriod { get; } = TimeSpan.FromMinutes(2);
    }
}
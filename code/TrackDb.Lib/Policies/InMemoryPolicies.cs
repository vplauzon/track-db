using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicies(
        int MaxBlocksPerTable = 5,
        int MaxUserDataRecords = 100,
        int MaxMetaDataRecords = 100,
        int MaxTombstonedRecords = 100,
        TimeSpan? MaxTombstonePeriod = null)
    {
        public static TimeSpan DefaultMaxTombstonePeriod { get; } = TimeSpan.FromMinutes(2);
    }
}
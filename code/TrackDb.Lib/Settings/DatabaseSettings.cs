using System;

namespace TrackDb.Lib.Settings
{
    public record DatabaseSettings(
        int MaxUnpersistedBlocksPerTable = 5,
        int MaxUnpersistedRecordsPerDb = 100,
        int MaxUnpersistedMetaDataRecordsPerTable = 50,
        int MaxTombstonedRecords = 100,
        TimeSpan? MaxTombstonePeriod = null)
    {
        public static TimeSpan DefaultMaxTombstonePeriod { get; } = TimeSpan.FromMinutes(2);
    }
}
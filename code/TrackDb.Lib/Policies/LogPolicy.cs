using System;

namespace TrackDb.Lib.Policies
{
    public record LogPolicy(
        StorageConfiguration? StorageConfiguration,
        TimeSpan BufferingTimeWindow,
        int MaxBatchSizeInBytes,
        int MinRecordCountBeforeCheckpoint,
        int MinTombstonePercentBeforeCheckpoint,
        int MinRecordCountPerCheckpointTransaction)
    {
        public static LogPolicy Create(
            StorageConfiguration? StorageConfiguration = null,
            TimeSpan? BufferingTimeWindow = null,
            int? MaxBatchSizeInBytes = null,
            int? MinRecordCountBeforeCheckpoint = null,
            int? MinTombstonePercentBeforeCheckpoint = null,
            int? MinRecordCountPerCheckpointTransaction = null)
        {
            return new LogPolicy(
                StorageConfiguration,
                BufferingTimeWindow ?? TimeSpan.FromSeconds(5),
                MaxBatchSizeInBytes ?? 8 * 1024 * 1024,
                MinRecordCountBeforeCheckpoint ?? 10000,
                MinTombstonePercentBeforeCheckpoint ?? 40,
                MinRecordCountPerCheckpointTransaction ?? 200);
        }
    }
}
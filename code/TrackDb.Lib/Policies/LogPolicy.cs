using Azure.Core;
using System;

namespace TrackDb.Lib.Policies
{
    public record LogPolicy(
        StorageConfiguration? StorageConfiguration,
        TimeSpan BufferingTimeWindow,
        int MaxBatchSizeInBytes)
    {
        public static LogPolicy Create(
            StorageConfiguration? StorageConfiguration = null,
            TimeSpan? BufferingTimeWindow = null,
            int? MaxBatchSizeInBytes = null)
        {
            return new LogPolicy(
                StorageConfiguration,
                BufferingTimeWindow ?? TimeSpan.FromSeconds(5),
                MaxBatchSizeInBytes ?? 8 * 1024 * 1024);
        }
    }
}
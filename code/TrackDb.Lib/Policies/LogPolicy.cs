using Azure.Core;
using System;

namespace TrackDb.Lib.Policies
{
    public record LogPolicy(
        StorageConfiguration? StorageConfiguration,
        TimeSpan BufferingTimeWindow)
    {
        public static LogPolicy Create(
            StorageConfiguration? StorageConfiguration = null,
            TimeSpan? BufferingTimeWindow = null)
        {
            return new LogPolicy(
                StorageConfiguration,
                BufferingTimeWindow ?? TimeSpan.FromSeconds(5));
        }
    }
}
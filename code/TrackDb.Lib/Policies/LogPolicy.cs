using System;

namespace TrackDb.Lib.Policies
{
    public record LogPolicy(
        Uri? LogFolderUri,
        TimeSpan BufferingTimeWindow)
    {
        public static LogPolicy Create(
            Uri? LogFolderUri = null,
            TimeSpan? BufferingTimeWindow = null)
        {
            return new LogPolicy(
                  LogFolderUri,
                  BufferingTimeWindow ?? TimeSpan.FromSeconds(5));
        }
    }
}
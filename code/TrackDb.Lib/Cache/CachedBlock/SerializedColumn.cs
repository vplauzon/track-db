using System;

namespace TrackDb.Lib.Cache.CachedBlock
{
    internal record SerializedColumn(
        int ItemCount,
        bool HasNulls,
        object? ColumnMinimum,
        object? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
using System;

namespace TrackDb.Lib.InMemory.Block
{
    internal record StatsSerializedColumn(
        int ItemCount,
        bool HasNulls,
        object? ColumnMinimum,
        object? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
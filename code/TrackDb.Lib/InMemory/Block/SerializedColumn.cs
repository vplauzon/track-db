using System;

namespace TrackDb.Lib.InMemory.Block
{
    internal record SerializedColumn(
        int ItemCount,
        bool HasNulls,
        object? ColumnMinimum,
        object? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
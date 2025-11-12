using System;

namespace TrackDb.Lib.InMemory.Block
{
    internal record ColumnStats(
        int ItemCount,
        bool HasNulls,
        object? ColumnMinimum,
        object? ColumnMaximum);
}
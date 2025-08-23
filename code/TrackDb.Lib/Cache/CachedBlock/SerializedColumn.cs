using System;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record SerializedColumn(
        int ItemCount,
        bool HasNulls,
        object? ColumnMinimum,
        object? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
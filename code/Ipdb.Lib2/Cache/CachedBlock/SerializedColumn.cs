using System;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record SerializedColumn(
        object? ColumnMinimum,
        object? ColumnMaximum,
        int ItemCount,
        ReadOnlyMemory<byte> Payload);
}
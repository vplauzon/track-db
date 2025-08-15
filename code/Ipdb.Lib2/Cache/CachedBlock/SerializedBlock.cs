using System;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record SerializedBlock(
        int ItemCount,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima,
        ReadOnlyMemory<byte> Payload);
}
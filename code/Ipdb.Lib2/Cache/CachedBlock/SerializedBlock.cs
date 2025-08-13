using System;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record SerializedBlock(
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima,
        ReadOnlyMemory<byte> Payload);
}
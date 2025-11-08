using System;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal record LongCompressedPackage(
        int ItemCount,
        bool HasNulls,
        long? ColumnMinimum,
        long? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
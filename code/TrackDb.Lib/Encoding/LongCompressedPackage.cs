using System;

namespace TrackDb.Lib.Encoding
{
    internal record LongCompressedPackage(
        int ItemCount,
        bool HasNulls,
        long? ColumnMinimum,
        long? ColumnMaximum,
        ReadOnlyMemory<byte> Payload);
}
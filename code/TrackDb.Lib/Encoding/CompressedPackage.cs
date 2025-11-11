using System;

namespace TrackDb.Lib.Encoding
{
    internal record CompressedPackage<T>(
        int ItemCount,
        bool HasNulls,
        T? ColumnMinimum,
        T? ColumnMaximum);
}
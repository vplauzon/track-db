using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    internal record SerializedBlockMetaData(
        int ItemCount,
        int Size,
        int BlockId,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima)
    {
        public static SerializedBlockMetaData FromMetaDataRecord(
            ReadOnlyMemory<object?> record)
        {
            var columnStats = record.Slice(0, record.Length - 3);
            var blockInfo = record.Slice(columnStats.Length).Span;
            var itemCount = ((int?)blockInfo[0])!.Value;
            var size = ((int?)blockInfo[1])!.Value;
            var blockId = ((int?)blockInfo[2])!.Value;

            if (columnStats.Length % 2 != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(record));
            }

            var columnEnumeration = Enumerable.Range(0, columnStats.Length / 2);
            var columnMinima = columnEnumeration
                .Select(i => columnStats.Span[i * 2 + 0])
                .ToImmutableArray();
            var columnMaxima = columnEnumeration
                .Select(i => columnStats.Span[i * 2 + 1])
                .ToImmutableArray();

            return new SerializedBlockMetaData(
                itemCount,
                size,
                blockId,
                columnMinima,
                columnMaxima);
        }

        public ReadOnlySpan<object?> CreateMetaDataRecord()
        {
            var metaData = ColumnMinima
                .Zip(ColumnMaxima)
                .Select(o => new object?[]
                {
                    o.First,
                    o.Second
                })
                .SelectMany(c => c)
                .Append(ItemCount)
                .Append(Size)
                .Append(BlockId)
                .ToArray();

            return metaData;
        }
    }
}
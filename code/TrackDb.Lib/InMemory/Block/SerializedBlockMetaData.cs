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
        IImmutableList<bool> ColumnHasNulls,
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

            if (columnStats.Length % 3 != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(record));
            }

            var columnEnumeration = Enumerable.Range(0, columnStats.Length / 3);
            var columnHasNulls = columnEnumeration
                .Select(i => ((bool?)columnStats.Span[i * 3])!.Value)
                .ToImmutableArray();
            var columnMinima = columnEnumeration
                .Select(i => columnStats.Span[i * 3 + 1])
                .ToImmutableArray();
            var columnMaxima = columnEnumeration
                .Select(i => columnStats.Span[i * 3 + 2])
                .ToImmutableArray();

            return new SerializedBlockMetaData(
                itemCount,
                size,
                blockId,
                columnHasNulls,
                columnMinima,
                columnMaxima);
        }

        public ReadOnlySpan<object?> CreateMetaDataRecord()
        {
            var metaData = ColumnHasNulls
                .Zip(ColumnMinima, ColumnMaxima)
                .Select(o => new object?[]
                {
                    o.First,
                    o.Second,
                    o.Third
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
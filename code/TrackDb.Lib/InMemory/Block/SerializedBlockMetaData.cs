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
        IImmutableList<bool> ColumnHasNulls,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima)
    {
        public static SerializedBlockMetaData FromMetaDataRecord(
            ReadOnlyMemory<object?> record,
            out int blockId)
        {
            var columnStats = record.Slice(0, record.Length - 3);
            var blockInfo = record.Slice(columnStats.Length).Span;
            var itemCount = ((int?)blockInfo[0])!.Value;
            var size = ((int?)blockInfo[1])!.Value;

            if (columnStats.Length % 3 != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(record));
            }
            blockId = ((int?)blockInfo[2])!.Value;

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
                columnHasNulls,
                columnMinima,
                columnMaxima);
        }

        public ReadOnlySpan<object?> CreateMetaDataRecord(int blockId)
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
                .Append(blockId)
                .ToArray();

            return metaData;
        }

        public static TableSchema CreateMetadataSchema(TableSchema tableSchema)
        {
            var metaDataColumns = tableSchema.Columns
                //  For each column we create a min, max & hasNulls column
                .Select(c => new[]
                {
                    new ColumnSchema($"$hasNulls-{c.ColumnName}", typeof(bool)),
                    new ColumnSchema($"$min-{c.ColumnName}", c.ColumnType),
                    new ColumnSchema($"$max-{c.ColumnName}", c.ColumnType)
                })
                //  We add the record-id columns
                .Append(
                [
                    new ColumnSchema("$hasNulls-$recordId", typeof(bool)),
                    new ColumnSchema("$min-$recordId", typeof(long)),
                    new ColumnSchema("$max-$recordId", typeof(long))
                ])
                //  We add the itemCount & block-id columns
                .Append(
                [
                    new ColumnSchema(MetadataColumns.ITEM_COUNT, typeof(int)),
                    new ColumnSchema(MetadataColumns.SIZE, typeof(int)),
                    new ColumnSchema(MetadataColumns.BLOCK_ID, typeof(int))
                ])
                //  We fan out the columns
                .SelectMany(c => c);
            var metaDataSchema = new TableSchema(
                $"$meta-{tableSchema.TableName}",
                metaDataColumns,
                Array.Empty<int>(),
                Array.Empty<int>());

            return metaDataSchema;
        }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;

namespace TrackDb.Lib.InMemory.Block
{
    internal record SerializedBlock(SerializedBlockMetaData MetaData, ReadOnlyMemory<byte> Payload)
    {
        #region Constructor
        public static SerializedBlock Create(
            int blockId,
            IImmutableList<SerializedColumn> columns)
        {
            var payload = CreateBlockPayload(columns);
            var metadata = new SerializedBlockMetaData(
                columns.First().ItemCount,
                payload.Span.Length * sizeof(byte),
                blockId,
                columns
                .Select(c => c.ColumnMinimum)
                .ToImmutableArray(),
                columns
                .Select(c => c.ColumnMaximum)
                .ToImmutableArray());

            return new(metadata, payload);
        }

        private static ReadOnlyMemory<byte> CreateBlockPayload(
            IImmutableList<SerializedColumn> serializedColumns)
        {
            var hasNullsPayload = BitPacker.Pack(
                serializedColumns.Select(c => Convert.ToUInt64(c.HasNulls)),
                serializedColumns.Count,
                1);
            var hasNullsPayloadSize = hasNullsPayload.Length * sizeof(byte);
            var payloadSizes = serializedColumns
                .Select(c => c.Payload.Length)
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * payloadSizes.Length;
            var blockPayload = new byte[
                hasNullsPayloadSize
                + payloadSizesSize
                + payloadSizes.Sum()];
            var hasNullsSpan = blockPayload.AsSpan().Slice(0, hasNullsPayloadSize);
            var sizeSpan = blockPayload.AsSpan().Slice(hasNullsPayloadSize, payloadSizesSize);

            hasNullsPayload.CopyTo(hasNullsSpan);
            for (int i = 0; i != payloadSizes.Length; ++i)
            {
                //  Write column payload size to the block header
                BinaryPrimitives.WriteUInt16LittleEndian(
                    sizeSpan.Slice(sizeof(short) * i, sizeof(short)),
                    (UInt16)payloadSizes[i]);
                //  Write column payload within block payload
                serializedColumns[i].Payload.CopyTo(
                    blockPayload.AsMemory().Slice(
                        hasNullsPayloadSize + payloadSizesSize + payloadSizes.Take(i).Sum(),
                        serializedColumns[i].Payload.Length));
            }

            return blockPayload;
        }
        #endregion

        public IEnumerable<SerializedColumn> CreateSerializedColumns()
        {
            var columnCount = MetaData.ColumnMinima.Count;
            var hasNullsPayloadSize = (columnCount + 7) / 8 * sizeof(byte); //  (N+7)/8 = ceil(N/8)
            var hasNullsPayload = Payload.Slice(0, hasNullsPayloadSize);
            var hasNulls = BitPacker.Unpack(hasNullsPayload.Span, columnCount, 1)
                .Select(i => Convert.ToBoolean(i))
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * columnCount;
            var payloadSizesPayload = Payload.Slice(hasNullsPayload.Length, payloadSizesSize);
            var columnPayloadSizes = Enumerable.Range(0, columnCount)
                .Select(i => payloadSizesPayload.Slice(i * sizeof(short), sizeof(short)))
                .Select(memory => BinaryPrimitives.ReadUInt16LittleEndian(memory.Span))
                .ToImmutableArray();
            var columnsPayload = Payload.Slice(hasNullsPayload.Length + payloadSizesSize);
            var serializedColumns = ImmutableArray<SerializedColumn>.Empty.ToBuilder();

            for (var i = 0; i != columnCount; i++)
            {
                var columnPayload = columnsPayload.Slice(0, columnPayloadSizes[i]);

                columnsPayload = columnsPayload.Slice(columnPayloadSizes[i]);
                serializedColumns.Add(new SerializedColumn(
                    MetaData.ItemCount,
                    hasNulls[i],
                    MetaData.ColumnMinima[i],
                    MetaData.ColumnMaxima[i],
                    columnPayload));
            }

            return serializedColumns.ToImmutable();
        }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    internal record SerializedBlock(SerializedBlockMetaData MetaData, ReadOnlyMemory<byte> Payload)
    {
        #region Constructor
        public static SerializedBlock Create(IImmutableList<SerializedColumn> columns)
        {
            var payload = CreateBlockPayload(columns);
            var metadata = new SerializedBlockMetaData(
                columns.First().ItemCount,
                payload.Span.Length*sizeof(byte),
                columns
                .Select(c => c.HasNulls)
                .ToImmutableArray(),
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
            var payloadSizes = serializedColumns
                .Select(c => c.Payload.Length)
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * payloadSizes.Length;
            var blockPayload = new byte[
                payloadSizesSize
                + payloadSizes.Sum()];
            var sizeSpan = blockPayload.AsSpan().Slice(0, payloadSizesSize);

            for (int i = 0; i != payloadSizes.Length; ++i)
            {
                //  Write column payload size to the block header
                BinaryPrimitives.WriteUInt16LittleEndian(
                    sizeSpan.Slice(sizeof(short) * i, sizeof(short)),
                    (UInt16)payloadSizes[i]);
                //  Write column payload within block payload
                serializedColumns[i].Payload.CopyTo(
                    blockPayload.AsMemory().Slice(
                        payloadSizesSize + payloadSizes.Take(i).Sum(),
                        serializedColumns[i].Payload.Length));
            }

            return blockPayload;
        }
        #endregion

        public IEnumerable<SerializedColumn> CreateSerializedColumns()
        {
            var columnCount = MetaData.ColumnHasNulls.Count;
            var payloadSizesSize = sizeof(short) * columnCount;
            var columnPayloadSizes = Enumerable.Range(0, columnCount)
                .Select(i => Payload.Slice(i * sizeof(short), sizeof(short)))
                .Select(memory => BinaryPrimitives.ReadUInt16LittleEndian(memory.Span))
                .ToImmutableArray();
            var columnsPayload = Payload.Slice(payloadSizesSize);
            var serializedColumns = ImmutableArray<SerializedColumn>.Empty.ToBuilder();

            for (var i = 0; i != columnCount; i++)
            {
                var columnPayload = columnsPayload.Slice(0, columnPayloadSizes[i]);

                columnsPayload = columnsPayload.Slice(columnPayloadSizes[i]);
                serializedColumns.Add(new SerializedColumn(
                    MetaData.ItemCount,
                    MetaData.ColumnHasNulls[i],
                    MetaData.ColumnMinima[i],
                    MetaData.ColumnMaxima[i],
                    columnPayload));
            }

            return serializedColumns.ToImmutable();
        }
    }
}
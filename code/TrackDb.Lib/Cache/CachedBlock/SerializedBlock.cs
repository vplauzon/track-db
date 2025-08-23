using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Cache.CachedBlock
{
    internal record SerializedBlock(SerializedBlockMetaData MetaData, ReadOnlyMemory<byte> Payload)
    {
        #region Constructor
        public SerializedBlock(IImmutableList<SerializedColumn> serializedColumns)
            : this(
                  new SerializedBlockMetaData(
                      serializedColumns.First().ItemCount,
                      serializedColumns
                      .Select(c => c.HasNulls)
                      .ToImmutableArray(),
                      serializedColumns
                      .Select(c => c.ColumnMinimum)
                      .ToImmutableArray(),
                      serializedColumns
                      .Select(c => c.ColumnMaximum)
                      .ToImmutableArray()),
                  CreateBlockPayload(serializedColumns))
        {
        }

        private static ReadOnlyMemory<byte> CreateBlockPayload(IImmutableList<SerializedColumn> serializedColumns)
        {
            var payloadSizes = serializedColumns
                .Select(c => (short)c.Payload.Length)
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * payloadSizes.Length;
            var blockPayload = new byte[
                payloadSizesSize
                + payloadSizes.Select(i => (int)i).Sum()];
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
                        payloadSizesSize + serializedColumns.Take(i).Select(c => c.Payload.Length).Sum(),
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

                serializedColumns.Add(new SerializedColumn(
                    MetaData.ItemCount,
                    MetaData.ColumnHasNulls[i],
                    MetaData.ColumnMinima[i],
                    MetaData.ColumnMaxima[i],
                    columnsPayload));
            }

            return serializedColumns.ToImmutable();
        }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using TrackDb.Lib.Storage;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Represents a serialized in-memory block with statistics.
    /// </summary>
    /// <remarks>
    /// Statistics are available when a block is serialized but isn't always persisted
    /// in tables.  For this reason, <see cref="SerializedBlock"/> is used to represent
    /// a block from a table.
    /// </remarks>
    /// <param name="ItemCount"></param>
    /// <param name="Size"></param>
    /// <param name="ColumnMinima"></param>
    /// <param name="ColumnMaxima"></param>
    /// <param name="Payload"></param>
    internal record StatsSerializedBlock(
        int ItemCount,
        int Size,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima,
        ReadOnlyMemory<byte> Payload)
    {
        #region Constructor
        public static StatsSerializedBlock Create(IImmutableList<StatsSerializedColumn> columns)
        {
            var payload = CreateBlockPayload(columns);
            var columnMinima = columns
                .Select(c => c.ColumnMinimum)
                .ToImmutableArray();
            var columnMaxima = columns
                .Select(c => c.ColumnMaximum)
                .ToImmutableArray();

            return new(
                columns.First().ItemCount,
                payload.Span.Length * sizeof(byte),
                columnMinima,
                columnMaxima,
                payload);
        }

        private static ReadOnlyMemory<byte> CreateBlockPayload(
            IImmutableList<StatsSerializedColumn> serializedColumns)
        {
            var hasNullsPayload = new byte[BitPacker.PackSize(serializedColumns.Count, 1)];
            var writer = new ByteWriter(hasNullsPayload);

            BitPacker.Pack(
                serializedColumns.Select(c => Convert.ToUInt64(c.HasNulls)),
                serializedColumns.Count,
                1,
                ref writer);

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
    }
}
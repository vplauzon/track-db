using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Represents a serialized in-memory block.
    /// </summary>
    /// <param name="ItemCount"></param>
    /// <param name="Columns"></param>
    internal record SerializedBlock(
        int ItemCount,
        IImmutableList<SerializedColumn> Columns)
    {
        /// <summary>
        /// Loads columns but not their data.  This way columns can be lazy loaded.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="payload"></param>
        /// <returns></returns>
        public static SerializedBlock Load(TableSchema schema, ReadOnlyMemory<byte> payload)
        {
            var columnCount = schema.Columns.Count;
            var hasNullsPayloadSize = (columnCount + 7) / 8 * sizeof(byte); //  (N+7)/8 = ceil(N/8)
            var hasNullsPayload = payload.Slice(0, hasNullsPayloadSize);
            var hasNulls = BitPacker.Unpack(hasNullsPayload.Span, columnCount, 1)
                .Select(i => Convert.ToBoolean(i))
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * columnCount;
            var payloadSizesPayload = payload.Slice(hasNullsPayload.Length, payloadSizesSize);
            var columnPayloadSizes = Enumerable.Range(0, columnCount)
                .Select(i => payloadSizesPayload.Slice(i * sizeof(short), sizeof(short)))
                .Select(memory => BinaryPrimitives.ReadUInt16LittleEndian(memory.Span))
                .ToImmutableArray();
            var columnsPayload = payload.Slice(hasNullsPayload.Length + payloadSizesSize);
            var serializedColumns = ImmutableArray<SerializedColumn>.Empty.ToBuilder();

            //  Get Item count
            throw new NotImplementedException();
            /*
            for (var i = 0; i != columnCount; i++)
            {
                var columnPayload = columnsPayload.Slice(0, columnPayloadSizes[i]);

                columnsPayload = columnsPayload.Slice(columnPayloadSizes[i]);
                serializedColumns.Add(new SerializedColumn(
                    ItemCount,
                    hasNulls[i],
                    columnPayload));
            }

            return new(ItemCount, serializedColumns.ToImmutable());
            */
        }
    }
}
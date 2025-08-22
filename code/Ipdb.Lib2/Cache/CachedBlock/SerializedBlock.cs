using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal record SerializedBlock(
        int ItemCount,
        IImmutableList<bool> ColumnHasNulls,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima,
        ReadOnlyMemory<byte> Payload)
    {
        #region Constructor
        public SerializedBlock(IImmutableList<SerializedColumn> serializedColumns)
            : this(
                  serializedColumns.First().ItemCount,
                  serializedColumns
                  .Select(c => c.HasNulls)
                  .ToImmutableArray(),
                  serializedColumns
                  .Select(c => c.ColumnMinimum)
                  .ToImmutableArray(),
                  serializedColumns
                  .Select(c => c.ColumnMaximum)
                  .ToImmutableArray(),
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

        public ReadOnlySpan<object?> GetMetaDataRecord(int blockId)
        {
            var metaData = ColumnHasNulls
                .Zip(ColumnMinima, ColumnMaxima)
                .Select(o => new object?[]
                {
                    o.First,
                    o.Second,
                    o.Third
                })
                .Append(new object?[]
                {
                    ItemCount,
                    blockId
                })
                .SelectMany(c => c)
                .ToArray();

            return metaData;
        }
    }
}
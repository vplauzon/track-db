using EasyCompressor;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
{
    internal static partial class Int64Codec
    {
        private static readonly ICompressor _zstdCompressor = ZstdSharpCompressor.Shared;

        /// <summary>
        /// <paramref name="values"/> is enumerated into three times.
        /// Since everything is transient, we do away with sanity checks
        /// such as magic number + version.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static SerializedColumn Compress(IEnumerable<long?> values)
        {
            if (values == null || !values.Any())
            {
                throw new ArgumentNullException(nameof(values));
            }
            var itemCount = values.Count();

            // Build validity bitmap (1 = valid, 0 = null)
            var bitmapBytes = (itemCount + 7) / 8;
            var bitmap = new byte[bitmapBytes];
            var nonNull = 0;
            var min = long.MaxValue;
            var max = long.MinValue;
            var i = 0;

            //  Populate bitmap + compute extrema
            foreach (var v in values)
            {
                if (v != null)
                {
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
                    nonNull++;
                    if (v < min)
                    {
                        min = v.Value;
                    }
                    if (v > max)
                    {
                        max = v.Value;
                    }
                }
                ++i;
            }

            // If all nulls, set min=0, bitWidth=0, no values section.
            var bitWidth = 0;

            if (nonNull > 0)
            {
                long range = (long)max - min; // non-negative
                while (range > 0)
                {
                    bitWidth++;
                    range >>= 1;
                }
                // We still store 64-bits deltas (fast path) — Zstd will compress them well.
                // If you want even smaller, you can bit-pack deltas using bitWidth; start simple first.
            }

            //  Prepare raw payload buffer (uncompressed)
            var extremeNulls = nonNull == itemCount || nonNull == 0;
            var headerSize = extremeNulls ? 0 : sizeof(int);
            var bitmapSize = extremeNulls ? 0 : bitmapBytes * sizeof(byte);
            var deltaSize = nonNull * sizeof(long);
            var payloadSize = headerSize + bitmapSize + nonNull * deltaSize;
            //  Everything from offset 5 onward is compressed; we will build it in a second buffer first
            //  to keep the compressor’s input contiguous.
            var payload = new byte[payloadSize];
            var payloadSpan = payload.AsSpan();
            var headerSpan = payloadSpan.Slice(0, headerSize);
            var bitmapSpan = payloadSpan.Slice(headerSize, bitmapSize);
            var deltaSpan = payloadSpan.Slice(headerSize + bitmapSize);

            //  Header
            if (!extremeNulls)
            {
                BinaryPrimitives.WriteInt32LittleEndian(headerSpan.Slice(0, sizeof(int)), nonNull);
                headerSpan = headerSpan.Slice(sizeof(int));
            }
            //  Header span should be 0 length here

            //  bitmap
            if (!extremeNulls)
            {
                bitmap.AsSpan().CopyTo(bitmapSpan);
            }

            // deltas (value - min) for non-nulls, as Int32 little-endian
            if (nonNull > 0)
            {
                foreach (var v in values)
                {
                    if (v != null)
                    {
                        var delta = v.Value - min;

                        BinaryPrimitives.WriteInt64LittleEndian(
                            deltaSpan.Slice(0, sizeof(long)),
                            delta);
                        deltaSpan = deltaSpan.Slice(sizeof(long));
                    }
                }
            }

            var compressedPayload = payload.Any()
                ? _zstdCompressor.Compress(payload)
                : Array.Empty<byte>();

            return new SerializedColumn(
                itemCount,
                nonNull < itemCount,
                nonNull == 0 ? null : min,
                nonNull == 0 ? null : max,
                compressedPayload);
        }

        public static IEnumerable<long?> Decompress(SerializedColumn column)
        {
            if (column.ColumnMinimum == null)
            {
                return Enumerable
                    .Range(0, column.ItemCount)
                    .Select(i => (long?)null);
            }
            else
            {
                var min = new Lazy<long?>(() => ((long?)column.ColumnMinimum)!.Value);
                var values = new long?[column.ItemCount];
                var bitmapBytes = (column.ItemCount + 7) / 8;
                var extremeNulls = !column.HasNulls || column.ColumnMinimum == null;
                var headerSize = extremeNulls ? 0 : sizeof(int);
                var bitmapSize = extremeNulls ? 0 : bitmapBytes * sizeof(byte);
                var compressedPayloadSpan = column.Payload.Span;
                var payload = _zstdCompressor.Decompress(compressedPayloadSpan.ToArray());
                var payloadSpan = new ReadOnlySpan<byte>(payload);
                var headerSpan = payloadSpan.Slice(0, headerSize);
                var nonNull = column.HasNulls
                    ? BinaryPrimitives.ReadInt32LittleEndian(headerSpan.Slice(0, sizeof(int)))
                    : column.ItemCount;
                var deltaSize = nonNull * sizeof(long);
                var bitmapSpan = payloadSpan.Slice(headerSize, bitmapSize);
                var deltaSpan = payloadSpan.Slice(headerSize + bitmapSize);

                //  Read deltas back
                for (var i = 0; i < column.ItemCount; i++)
                {
                    var valid = column.HasNulls
                        ? ((bitmapSpan[i >> 3] >> (i & 7)) & 1) != 0
                        : true;

                    if (valid)
                    {
                        var delta = BinaryPrimitives.ReadInt64LittleEndian(
                            deltaSpan.Slice(0, sizeof(long)));

                        deltaSpan = deltaSpan.Slice(sizeof(long));
                        values[i] = min.Value + delta;
                    }
                    else
                    {
                        values[i] = null;
                    }
                }

                return values;
            }
        }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;
using TrackDb.Lib.Storage;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    /// <summary>
    /// Compression of a sequence of nullable 64 bits integers into a byte array.
    /// The byte array relies on the metadata carried by <see cref="SerializedColumn"/>.
    /// The structure of the metadata is:  <c>nonNull</c> (UInt16), null bitmap & deltas.
    /// 
    /// <c>nonNull</c> is present iif we are not in "extreme null regime".  This is defined when
    /// either all items are null or none are.
    /// 
    /// Similarly, null bitmaps is present iif we are not in extreme null regime.
    /// 
    /// Deltas are present only when the min and max are different.
    /// </summary>
    internal static class Int64Codec
    {
        #region Compress
        /// <summary>
        /// <paramref name="values"/> is enumerated into three times.
        /// Since everything is transient, we do away with sanity checks
        /// such as magic number + version.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static LongCompressedPackage Compress(IEnumerable<long?> values)
        {
            if (values == null || !values.Any())
            {
                throw new ArgumentNullException(nameof(values));
            }
            var bufferWriter = new ByteWriter(new byte[4096]);
            var draftWriter = new ByteWriter(new byte[4096]);
            var itemCount = values.Count();

            if (itemCount > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Sequence is too large:  '{itemCount}'");
            }

            // Build validity bitmap (1 = valid, 0 = null)
            var bitmapBytes = (itemCount + 7) / 8;
            //  We need to write the bitmap on a draft as we don
            var bitmap = draftWriter.VirtualByteSpanForward(bitmapBytes);
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
                    ++nonNull;
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

            var extremeNullRegime = nonNull == itemCount || nonNull == 0;

            bufferWriter.WriteUInt16((ushort)nonNull);
            if (nonNull != 0)
            {
                bufferWriter.WriteInt64(min);
                bufferWriter.WriteInt64(max);
            }
            if (!extremeNullRegime)
            {
                bufferWriter.CopyFrom(bitmap);
            }
            if (nonNull != 0 && min != max)
            {
                var packedDeltas = BitPacker.Pack(
                    values.Where(v => v.HasValue).Select(v => ToZeroBase(v!.Value, min)),
                    nonNull,
                    ToZeroBase(max, min));

                bufferWriter.WriteBytes(packedDeltas);
            }

            return new LongCompressedPackage(
                itemCount,
                nonNull < itemCount,
                nonNull == 0 ? null : min,
                nonNull == 0 ? null : max,
                bufferWriter.ToArray());
        }

        private static ulong ToZeroBase(long value, long min)
        {
            // Fast path: if the subtraction won't overflow
            if ((value >= 0 && min >= 0) || (value <= 0 && min <= 0) ||
                (value >= min && value - min >= 0))
            {
                return (ulong)(value - min);
            }

            // Slow path: use BigInteger for large ranges
            return (ulong)(new BigInteger(value) - new BigInteger(min));
        }
        #endregion

        #region Decompress
        public static IEnumerable<long?> Decompress(
            int itemCount,
            bool hasNulls,
            ReadOnlyMemory<byte> payload)
        {
            var payloadSpan = payload.Span;
            var nonNullSize = sizeof(UInt16);
            var nonNullSpan = payloadSpan.Slice(0, nonNullSize);
            var nonNull = BinaryPrimitives.ReadUInt16LittleEndian(nonNullSpan);
            var extremeNullRegime = !hasNulls || nonNull == 0;
            var extremaSize = nonNull == 0 ? 0 : 2 * sizeof(long);
            var extremaSpan = payloadSpan.Slice(nonNullSpan.Length, extremaSize);
            long? columnMinimum = extremaSpan.Length == 0
                ? null
                : BinaryPrimitives.ReadInt64LittleEndian(extremaSpan.Slice(0, sizeof(long)));
            long? columnMaximum = extremaSpan.Length == 0
                ? null
                : BinaryPrimitives.ReadInt64LittleEndian(extremaSpan.Slice(sizeof(long)));

            if (!hasNulls && object.Equals(columnMinimum, columnMaximum))
            {   //  Only one value
                return Enumerable.Range(0, itemCount)
                    .Select(i => (long?)columnMinimum);
            }
            else if (columnMinimum == null)
            {   //  Only nulls
                return Enumerable.Range(0, itemCount)
                    .Select(i => (long?)null);
            }
            else
            {
                var bitmapBytes = (itemCount + 7) / 8;
                var bitmapSize = extremeNullRegime ? 0 : bitmapBytes * sizeof(byte);
                var bitmapSpan = payloadSpan.Slice(nonNullSize + extremaSize, bitmapSize);
                var packedDeltaSpan = payloadSpan.Slice(nonNullSize + extremaSize + bitmapSize);
                var min = (long)columnMinimum!;
                var max = (long)columnMaximum!;
                var hasDeltas = min != max;
                var deltas = hasDeltas
                    ? BitPacker.Unpack(packedDeltaSpan, nonNull, ToZeroBase(max, min))
                    : Array.Empty<ulong>();
                var values = new long?[itemCount];
                var deltaI = 0;

                //  Read deltas back
                for (var i = 0; i < itemCount; i++)
                {
                    var isNotNull = hasNulls
                        ? ((bitmapSpan[i >> 3] >> (i & 7)) & 1) != 0
                        : true;

                    if (isNotNull)
                    {
                        var delta = hasDeltas ? deltas[deltaI++] : 0UL;

                        values[i] = FromZeroBase(delta, min);
                    }
                    else
                    {
                        values[i] = null;
                    }
                }

                return values;
            }
        }

        private static long FromZeroBase(ulong delta, long min)
        {
            // Fast path: if the addition won't overflow
            if ((delta <= long.MaxValue && min >= 0) ||
                (delta <= (ulong)(long.MaxValue - min)))
            {
                return (long)delta + min;
            }

            // Slow path: use BigInteger for large ranges
            return (long)(new BigInteger(delta) + new BigInteger(min));
        }
        #endregion
    }
}
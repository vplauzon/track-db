using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.Encoding
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
                BitPacker.Pack(
                    values.Where(v => v.HasValue).Select(v => ToZeroBase(v!.Value, min)),
                    nonNull,
                    ToZeroBase(max, min),
                    ref bufferWriter);
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
            var bufferReader = new ByteReader(payload.Span);
            var nonNull = bufferReader.ReadUInt16();
            var extremeNullRegime = !hasNulls || nonNull == 0;
            long? columnMinimum = nonNull == 0 ? null : bufferReader.ReadInt64();
            long? columnMaximum = nonNull == 0 ? null : bufferReader.ReadInt64();

            if (columnMinimum == null)
            {   //  Only nulls
                return Enumerable.Range(0, itemCount)
                    .Select(i => (long?)null);
            }
            else
            {
                var min = (long)columnMinimum!;
                var max = (long)columnMaximum!;

                if (!hasNulls && min == max)
                {   //  Only one value
                    return Enumerable.Range(0, itemCount)
                        .Select(i => (long?)columnMinimum);
                }
                else
                {
                    var bitmapBytes = (itemCount + 7) / 8;
                    var bitmapSize = extremeNullRegime ? 0 : bitmapBytes * sizeof(byte);
                    var bitmapSpan = bufferReader.SpanForward(bitmapSize);
                    var values = new long?[itemCount];

                    if (min != max)
                    {   //  Use delta
                        var maxDeltaValue = ToZeroBase(max, min);
                        var packedDeltaSpan = bufferReader.SpanForward(BitPacker.PackSize(nonNull, maxDeltaValue));
                        var deltas = BitPacker.Unpack(packedDeltaSpan, nonNull, maxDeltaValue);
                        var i = 0;

                        //  Read deltas back
                        foreach (var deltaValue in deltas)
                        {
                            var isNotNull = false;

                            while (!isNotNull)
                            {
                                isNotNull = hasNulls
                                    ? ((bitmapSpan[i >> 3] >> (i & 7)) & 1) != 0
                                    : true;

                                if (isNotNull)
                                {
                                    values[i] = FromZeroBase(deltaValue, min);
                                }
                                else
                                {
                                    values[i] = null;
                                }
                                ++i;
                            }
                        }
                    }
                    else
                    {   //  Constant (min=max) deltas
                        for (var i = 0; i != itemCount; ++i)
                        {
                            values[i] = ((bitmapSpan[i >> 3] >> (i & 7)) & 1) != 0
                                ? min
                                : null;
                        }
                    }

                    return values;
                }
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
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Numerics;

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
        #region Compute Size
        /// <summary>Compute incremental serialization sizes.</summary>
        /// <param name="storedValues"></param>
        /// <param name="nullValue"></param>
        /// <param name="maxSize"></param>
        /// <param name="sizes"></param>
        /// <returns>Number of records actually input in <paramref name="sizes"/>.</returns>
        public static int ComputeSerializationSizes(
            scoped ReadOnlySpan<long> storedValues,
            long nullValue,
            Span<int> sizes,
            int maxSize)
        {
            var minValue = long.MaxValue;
            var maxValue = long.MinValue;
            var nonNull = 0;

            for (var i = 0; i != storedValues.Length; ++i)
            {
                var value = storedValues[i];

                if (value != nullValue)
                {
                    ++nonNull;
                    minValue = Math.Min(minValue, value);
                    maxValue = Math.Max(maxValue, value);
                }

                var extremeNullRegime = nonNull == (i + 1) || nonNull == 0;
                var size =
                    sizeof(short)   //  nonNull
                    + (nonNull == 0 ? 0 : 2 * sizeof(long))  //  min+max
                    + (extremeNullRegime    //  Bit map
                    ? 0
                    : BitPacker.PackSize(i + 1, 1))
                    + (nonNull != 0 && minValue != maxValue //  Delta values
                    ? BitPacker.PackSize(nonNull, (ulong)(maxValue - minValue))
                    : 0);

                if (size >= maxSize)
                {
                    return i;
                }
                sizes[i] = size;
            }

            return storedValues.Length;
        }
        #endregion

        #region Compress
        /// <summary>
        /// Since everything is transient, we do away with sanity checks
        /// such as magic number + version.
        /// </summary>
        /// <param name="values"></param>
        /// <param name="nullValue"></param>
        /// <param name="writer"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static CompressedPackage<long> Compress(
            scoped ReadOnlySpan<long> values,
            long nullValue,
            ref ByteWriter writer)
        {
            if (values.Length == 0)
            {
                throw new ArgumentException("Sequence can't be empty", nameof(values));
            }
            if (values.Length > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Sequence is too large ({values.Length})");
            }

            (var nonNull, var min, var max) = ComputeExtremas(values, nullValue);
            var extremeNullRegime = nonNull == values.Length || nonNull == 0;

            writer.WriteUInt16((ushort)nonNull);
            if (nonNull != 0)
            {
                writer.WriteInt64(min);
                writer.WriteInt64(max);
            }
            if (!extremeNullRegime)
            {   //  Pack bitmap
                Span<ulong> bitmap = values.Length <= 1024
                    ? stackalloc ulong[values.Length]
                    : new ulong[values.Length];

                FillBitmap(values, nullValue, bitmap);
                BitPacker.Pack(bitmap, 1, ref writer);
            }
            if (nonNull != 0 && min != max)
            {   //  Pack deltas
                Span<ulong> tempData = nonNull <= 1024
                    ? stackalloc ulong[nonNull]
                    : new ulong[nonNull];

                FillNonNull(values, nullValue, min, tempData);
                BitPacker.Pack(tempData, (ulong)(max - min), ref writer);
            }

            return new CompressedPackage<long>(
                values.Length,
                nonNull < values.Length,
                nonNull == 0 ? nullValue : min,
                nonNull == 0 ? nullValue : max);
        }

        private static void FillNonNull(
            ReadOnlySpan<long> values,
            long nullValue,
            long minValue,
            Span<ulong> nonNullData)
        {
            var i = 0;

            for (var j = 0; j != values.Length; ++j)
            {
                if (values[j] != nullValue)
                {
                    nonNullData[i] = (ulong)(ToZeroBase(values[j], minValue));
                    ++i;
                }
            }
        }

        private static void FillBitmap(
            ReadOnlySpan<long> values,
            long nullValue,
            Span<ulong> bitmap)
        {
            for (var i = 0; i != values.Length; i++)
            {
                bitmap[i] = values[i] == nullValue ? (ulong)0 : 1;
            }
        }

        private static (int NonNull, long Min, long Max) ComputeExtremas(
            ReadOnlySpan<long> values,
            long nullValue)
        {
            int nonNull = 0;
            long min = long.MaxValue, max = long.MinValue;

            //  Compute extrema & nonNull
            for (var i = 0; i != values.Length; ++i)
            {
                if (values[i] != nullValue)
                {
                    ++nonNull;
                    if (values[i] < min)
                    {
                        min = values[i];
                    }
                    if (values[i] > max)
                    {
                        max = values[i];
                    }
                }
            }

            return (nonNull, min, max);
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
        public static void Decompress(
            ref ByteReader payloadReader,
            scoped Span<long> values,
            long nullValue)
        {
            var nonNull = payloadReader.ReadUInt16();
            var hasNulls = nonNull != values.Length;
            var extremeNullRegime = !hasNulls || nonNull == 0;

            if (nonNull > values.Length)
            {
                throw new InvalidDataException(
                    $"Non-null ({nonNull}) should be <= to item count ({values.Length})");
            }
            if (nonNull == 0)
            {   //  Only nulls
                values.Fill(nullValue);
            }
            else
            {
                var min = payloadReader.ReadInt64();
                var max = payloadReader.ReadInt64();

                if (!hasNulls && min == max)
                {   //  Only one value
                    values.Fill(min);
                }
                else
                {
                    if (hasNulls)
                    {
                        var bitmapPackedSpan = payloadReader.SliceForward(
                            BitPacker.PackSize(values.Length, 1));
                        Span<ulong> bitmapUnpackedSpan = stackalloc ulong[values.Length];

                        BitPacker.Unpack(bitmapPackedSpan, 1, bitmapUnpackedSpan);
                        if (min != max)
                        {   //  Use delta
                            var maxDeltaValue = ToZeroBase(max, min);
                            var deltaPackedSpan = payloadReader.SliceForward(
                                BitPacker.PackSize(nonNull, maxDeltaValue));
                            Span<ulong> deltaUnpackedSpan = stackalloc ulong[nonNull];
                            var deltaIndex = 0;

                            BitPacker.Unpack(deltaPackedSpan, maxDeltaValue, deltaUnpackedSpan);
                            for (var i = 0; i != values.Length; ++i)
                            {
                                values[i] = bitmapUnpackedSpan[i] != 0
                                    ? FromZeroBase(deltaUnpackedSpan[deltaIndex++], min)
                                    : nullValue;
                            }
                        }
                        else
                        {   //  Constant (min=max) deltas
                            for (var i = 0; i != values.Length; ++i)
                            {
                                values[i] = bitmapUnpackedSpan[i] != 0
                                    ? min
                                    : nullValue;
                            }
                        }
                    }
                    else
                    {
                        if (min != max)
                        {   //  Use delta
                            var maxDeltaValue = ToZeroBase(max, min);
                            var deltaPackedSpan = payloadReader.SliceForward(
                                BitPacker.PackSize(nonNull, maxDeltaValue));
                            Span<ulong> deltaUnpackedSpan = stackalloc ulong[nonNull];
                            var deltaIndex = 0;

                            BitPacker.Unpack(deltaPackedSpan, maxDeltaValue, deltaUnpackedSpan);
                            for (var i = 0; i != values.Length; ++i)
                            {
                                values[i] = FromZeroBase(deltaUnpackedSpan[deltaIndex++], min);
                            }
                        }
                        else
                        {   //  Constant (min=max) deltas
                            values.Fill(min);
                        }
                    }
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
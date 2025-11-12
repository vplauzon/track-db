using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
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
        /// <paramref name="values"/> is enumerated into multiple times:  better not
        /// involve heavy compute.
        /// Since everything is transient, we do away with sanity checks
        /// such as magic number + version.
        /// </summary>
        /// <param name="values"></param>
        /// <param name="writer"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static CompressedPackage<long?> Compress(IEnumerable<long?> values, ref ByteWriter writer)
        {
            if (values == null || !values.Any())
            {
                throw new ArgumentNullException(nameof(values));
            }

            (var itemCount, var nonNull, var min, var max) = ComputeExtremas(values);

            if (itemCount > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Sequence is too large ({values.Count()})");
            }

            var extremeNullRegime = nonNull == itemCount || nonNull == 0;

            writer.WriteUInt16((ushort)nonNull);
            if (nonNull != 0)
            {
                writer.WriteInt64(min);
                writer.WriteInt64(max);
            }
            if (!extremeNullRegime)
            {
                BitPacker.Pack(
                    values.Select(v => Convert.ToUInt64(v != null)),
                    itemCount,
                    1,
                    ref writer);
            }
            if (nonNull != 0 && min != max)
            {   //  Pack deltas
                BitPacker.Pack(
                    values.Where(v => v.HasValue).Select(v => ToZeroBase(v!.Value, min)),
                    nonNull,
                    ToZeroBase(max, min),
                    ref writer);
            }

            return new CompressedPackage<long?>(
                itemCount,
                nonNull < itemCount,
                nonNull == 0 ? null : min,
                nonNull == 0 ? null : max);
        }

        private static (int ItemCount, int NonNull, long Min, long Max) ComputeExtremas(
            IEnumerable<long?> values)
        {
            int itemCount = 0, nonNull = 0;
            long min = int.MaxValue, max = int.MinValue;

            //  Compute extrema & nonNull
            foreach (var v in values)
            {
                if (v != null)
                {
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
                ++itemCount;
            }

            return (itemCount, nonNull, min, max);
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
            ReadOnlySpan<byte> payload)
        {
            var reader = new ByteReader(payload);
            var nonNull = reader.ReadUInt16();
            var extremeNullRegime = !hasNulls || nonNull == 0;
            long? columnMinimum = nonNull == 0 ? null : reader.ReadInt64();
            long? columnMaximum = nonNull == 0 ? null : reader.ReadInt64();

            if (nonNull > itemCount)
            {
                throw new InvalidDataException(
                    $"Non-null ({nonNull}) should be <= to item count ({itemCount})");
            }
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
                    var values = new long?[itemCount];

                    if (hasNulls)
                    {
                        var bitmapValues = BitPacker.Unpack(
                            reader.SpanForward(BitPacker.PackSize(itemCount, 1)),
                            itemCount,
                            1);

                        if (min != max)
                        {   //  Use delta
                            var maxDeltaValue = ToZeroBase(max, min);
                            var deltas = BitPacker.Unpack(
                                reader.SpanForward(BitPacker.PackSize(nonNull, maxDeltaValue)),
                                nonNull,
                                maxDeltaValue);
                            var deltaIndex = 0;

                            for (var i = 0; i != itemCount; ++i)
                            {
                                values[i] = Convert.ToBoolean(bitmapValues[i])
                                    ? FromZeroBase(deltas[deltaIndex++], min)
                                    : null;
                            }
                        }
                        else
                        {   //  Constant (min=max) deltas
                            for (var i = 0; i != itemCount; ++i)
                            {
                                values[i] = Convert.ToBoolean(bitmapValues[i])
                                    ? min
                                    : null;
                            }
                        }
                    }
                    else
                    {
                        if (min != max)
                        {   //  Use delta
                            var maxDeltaValue = ToZeroBase(max, min);
                            var deltas = BitPacker.Unpack(
                                reader.SpanForward(BitPacker.PackSize(nonNull, maxDeltaValue)),
                                nonNull,
                                maxDeltaValue);

                            for (var i = 0; i != itemCount; ++i)
                            {
                                values[i] = FromZeroBase(deltas[i], min);
                            }
                        }
                        else
                        {   //  Constant (min=max) deltas
                            for (var i = 0; i != itemCount; ++i)
                            {
                                values[i] = min;
                            }
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
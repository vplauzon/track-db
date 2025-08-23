using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;

namespace TrackDb.Lib.Cache.CachedBlock.SpecializedColumn
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
    internal static partial class Int64Codec
    {
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

            if (itemCount > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Sequence is too large:  '{itemCount}'");
            }

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
            var nonNullSize = extremeNullRegime ? 0 : sizeof(UInt16);
            var bitmapSize = extremeNullRegime ? 0 : bitmapBytes * sizeof(byte);
            var packedDeltas = nonNull == 0 || min == max ? Array.Empty<byte>() : BitPacker.Pack(
                values.Where(v => v.HasValue).Select(v => ToZeroBase(v!.Value, min)),
                nonNull,
                ToZeroBase(max, min));
            var payloadSize = nonNullSize + bitmapSize + packedDeltas.Length * sizeof(byte);
            var payload = new byte[payloadSize];
            var payloadSpan = payload.AsSpan();
            var nonNullSpan = payloadSpan.Slice(0, nonNullSize);
            var bitmapSpan = payloadSpan.Slice(nonNullSize, bitmapSize);
            var packedDeltaSpan = payloadSpan.Slice(nonNullSize + bitmapSize);

            //  Non-Null
            if (nonNullSpan.Length != 0)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(nonNullSpan.Slice(0, sizeof(UInt16)), (UInt16)nonNull);
            }
            //  bitmap
            if (bitmapSpan.Length != 0)
            {
                bitmap.AsSpan().CopyTo(bitmapSpan);
            }
            // deltas (value - min) for non-nulls
            if (packedDeltaSpan.Length != 0)
            {
                packedDeltas.CopyTo(packedDeltaSpan);
            }

            return new SerializedColumn(
                itemCount,
                nonNull < itemCount,
                nonNull == 0 ? null : min,
                nonNull == 0 ? null : max,
                payload);
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

        public static IEnumerable<long?> Decompress(SerializedColumn column)
        {
            if (!column.HasNulls && object.Equals(column.ColumnMinimum, column.ColumnMaximum))
            {   //  Only one value
                return Enumerable.Range(0, column.ItemCount)
                    .Select(i => (long?)column.ColumnMinimum);
            }
            else
            if (column.ColumnMinimum == null)
            {   //  Only nulls
                return Enumerable.Range(0, column.ItemCount)
                    .Select(i => (long?)null);
            }
            else
            {
                var extremeNullRegime = !column.HasNulls || column.ColumnMinimum == null;
                var hasDeltas = !object.Equals(column.ColumnMinimum, column.ColumnMaximum);
                var payloadSpan = column.Payload.Span;
                var nonNullSize = extremeNullRegime ? 0 : sizeof(UInt16);
                var nonNullSpan = payloadSpan.Slice(0, nonNullSize);
                var nonNull = nonNullSpan.Length != 0
                    ? BinaryPrimitives.ReadUInt16LittleEndian(nonNullSpan.Slice(0, sizeof(UInt16)))
                    : column.HasNulls
                    ? 0
                    : column.ItemCount;
                var bitmapBytes = (column.ItemCount + 7) / 8;
                var bitmapSize = extremeNullRegime ? 0 : bitmapBytes * sizeof(byte);
                var bitmapSpan = payloadSpan.Slice(nonNullSize, bitmapSize);
                var packedDeltaSpan = payloadSpan.Slice(nonNullSize + bitmapSize);
                var min = (long)column.ColumnMinimum!;
                var max = (long)column.ColumnMaximum!;
                var deltas = hasDeltas
                    ? BitPacker.Unpack(packedDeltaSpan, nonNull, ToZeroBase(max, min))
                    : Array.Empty<ulong>();
                var values = new long?[column.ItemCount];
                var deltaI = 0;

                //  Read deltas back
                for (var i = 0; i < column.ItemCount; i++)
                {
                    var valid = column.HasNulls
                        ? ((bitmapSpan[i >> 3] >> (i & 7)) & 1) != 0
                        : true;

                    if (valid)
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
    }
}

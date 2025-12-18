using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Compression of a sequence of nullable strings into a byte array.
    /// 
    /// This codec assumes the strings are relatively short and low cardinality of unique values.
    /// </summary>
    internal static class StringCodec
    {
        #region Compute Size
        public static int ComputeSerializationSizes(
            ReadOnlySpan<string?> storedValues,
            Span<int> sizes,
            int maxSize)
        {
            var uniqueValues = new HashSet<string>();
            var valueSequenceLength = 0;
            var valueSequenceMax = (ulong)0;

            for (var i = 0; i != storedValues.Length; ++i)
            {
                var value = storedValues[i];

                if (value != null)
                {
                    if (uniqueValues.Add(value))
                    {
                        valueSequenceLength += value.Length + 1;
                        if (value.Length != 0)
                        {
                            ulong localMax = 0;

                            foreach (var c in value)
                            {
                                if ((ulong)c > localMax)
                                {
                                    localMax = (ulong)c;
                                }
                            }

                            valueSequenceMax = Math.Max(valueSequenceMax, (ulong)(localMax + 1));
                        }
                    }
                }
                if (uniqueValues.Count != 0)
                {
                    var size = 
                        sizeof(ushort)  //  Value sequence count
                        + sizeof(byte)  //  Value sequence max
                        + BitPacker.PackSize(valueSequenceLength, valueSequenceMax) //  Value sequence
                        + BitPacker.PackSize(i + 1, (ulong)uniqueValues.Count);  //  indexes

                    if (size >= maxSize)
                    {
                        return i;
                    }
                    sizes[i] = size;
                }
                else
                {
                    sizes[i] = sizeof(ushort);  //  Write 0
                }
            }

            return storedValues.Length;
        }
        #endregion

        #region Compress
        /// <summary>
        /// <paramref name="values"/> is enumerated into multiple times.
        /// The general payload format is the following:
        /// <list type="bullet">
        /// <item>Unique value count (short).</item>
        /// <item>Min character value (short).</item>
        /// <item>Max character value (short).</item>
        /// <item>Unique values payload length.</item>
        /// <item>Unique values sequence length (i.e. total number of characters).</item>
        /// <item>
        /// Unique values int-64 codec compressed payload (as a sequence of characters punctuated by zeros).
        /// </item>
        /// <item>Index of unique values payload length.</item>
        /// <item>Index of unique values (-1 for null) int-64 codec compressed payload.</item>
        /// </list>
        /// 
        /// There are a few special cases on top of the general one.
        /// 
        /// If there are only null values, no payload is required.
        /// 
        /// If there are 2 unique values, they are in the min & max of the column, so no need to encode the
        /// values, just the indexes.
        /// 
        /// If there is 1 unique value and no null, no payload is required.
        /// </summary>
        /// <param name="values"></param>
        /// <param name="writer"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static CompressedPackage<string?> Compress(
            ReadOnlySpan<string?> values,
            ref ByteWriter writer)
        {
            if (values.Length == 0)
            {
                throw new ArgumentException("Can't have empty sequence", nameof(values));
            }
            if (values.Length > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Sequence is too large ({values.Length})");
            }

            (var orderedUniqueValues, var hasNulls) = ScanStats(values);

            if (orderedUniqueValues.Count > 0)
            {
                WriteUniqueValues(orderedUniqueValues, ref writer);
                WriteIndexes(values, orderedUniqueValues, ref writer);

                return new(
                    values.Length,
                    hasNulls,
                    orderedUniqueValues.First(),
                    orderedUniqueValues.Last());
            }
            else
            {   //  Only nulls
                writer.WriteUInt16((ushort)0);

                return new(
                    values.Length,
                    true,
                    null,
                    null);
            }
        }

        private static void WriteUniqueValues(
            IImmutableList<string> uniqueValues,
            ref ByteWriter writer)
        {
            var valuesSequenceLength = uniqueValues
                .Sum(v => v.Length + 1);
            Span<ulong> valuesSequenceSpan = valuesSequenceLength <= 1024
                ? stackalloc ulong[valuesSequenceLength]
                : new ulong[valuesSequenceLength];
            ulong valuesSequenceMax = 0;
            var j = 0;

            for (var i = 0; i != uniqueValues.Count; ++i)
            {
                foreach (var c in uniqueValues[i])
                {
                    var value = (ulong)c + 1;

                    valuesSequenceSpan[j++] = value;
                    valuesSequenceMax = Math.Max(valuesSequenceMax, value);
                }
                //  We punctuate the string by a value zero
                valuesSequenceSpan[j++] = 0;
            }

            writer.WriteUInt16((ushort)valuesSequenceLength);
            writer.WriteByte((byte)valuesSequenceMax);
            BitPacker.Pack(valuesSequenceSpan, valuesSequenceMax, ref writer);
        }

        private static void WriteIndexes(
            ReadOnlySpan<string?> values,
            IImmutableList<string> uniqueValues,
            ref ByteWriter writer)
        {
            Span<ulong> indexesSpan = values.Length <= 1024
                ? stackalloc ulong[values.Length]
                : new ulong[values.Length];
            var revertDictionary = new Dictionary<string, ushort>(uniqueValues.Count);

            //  More efficient than LINQ expression
            for (var i = (ushort)0; i != uniqueValues.Count; ++i)
            {
                revertDictionary[uniqueValues[i]] = (ushort)(i + 1);
            }
            for (var i = 0; i < values.Length; ++i)
            {
                var value = values[i];

                indexesSpan[i] = value == null ? 0 : (ulong)revertDictionary[value];
            }
            BitPacker.Pack(indexesSpan, (ulong)uniqueValues.Count, ref writer);
        }

        private static (IImmutableList<string> OrderedUniqueValues, bool HasNulls) ScanStats(
            ReadOnlySpan<string?> values)
        {
            var uniqueValues = new HashSet<string>();
            var hasNulls = false;

            foreach (var value in values)
            {
                if (value == null)
                {
                    hasNulls = true;
                }
                else
                {
                    uniqueValues.Add(value);
                }
            }

            //  Order is important as the min and max are later deduced from the start and end
            return (uniqueValues.Order().ToImmutableArray(), hasNulls);
        }
        #endregion

        #region Decompress
        public static void Decompress(ref ByteReader payloadReader, Span<string?> values)
        {
            List<string> BreakStrings(ReadOnlySpan<ulong> valueSequence)
            {
                var values = new List<string>();
                Span<char> charArray = valueSequence.Length <= 1024
                    ? stackalloc char[valueSequence.Length]
                    : new char[valueSequence.Length];
                var i = 0;

                foreach (var value in valueSequence)
                {
                    if (value == 0)
                    {
                        values.Add(new string(charArray.Slice(0, i)));
                        i = 0;
                    }
                    else
                    {
                        charArray[i++] = (char)(value - 1);
                    }
                }

                return values;
            }

            var valuesSequenceLength = payloadReader.ReadUInt16();

            if (valuesSequenceLength > 0)
            {
                //  Values sequence
                var valuesSequenceMax = payloadReader.ReadByte();
                var valuesSequencePackedSpan = payloadReader.SliceForward(
                    BitPacker.PackSize(valuesSequenceLength, valuesSequenceMax));
                Span<ulong> valueSequenceUnpackedSpan = valuesSequenceLength <= 1024
                    ? stackalloc ulong[valuesSequenceLength]
                    : new ulong[valuesSequenceLength];

                BitPacker.Unpack(
                    valuesSequencePackedSpan,
                    valuesSequenceMax,
                    valueSequenceUnpackedSpan);

                var uniqueValues = BreakStrings(valueSequenceUnpackedSpan);
                //  Indexes
                Span<ulong> indexesUnpackedSpan = values.Length <= 1024
                    ? stackalloc ulong[values.Length]
                    : new ulong[values.Length];
                var indexesPackedSpan = payloadReader.SliceForward(BitPacker.PackSize(
                    indexesUnpackedSpan.Length,
                    (ulong)uniqueValues.Count));

                BitPacker.Unpack(
                    indexesPackedSpan,
                    (ulong)uniqueValues.Count,
                    indexesUnpackedSpan);
                for (var i = 0; i != indexesUnpackedSpan.Length; ++i)
                {
                    if (indexesUnpackedSpan[i] == 0)
                    {
                        values[i] = null;
                    }
                    else
                    {
                        var index = (int)indexesUnpackedSpan[i] - 1;
                        var value = uniqueValues[index];

                        values[i] = value;
                    }
                }
            }
            else
            {
                values.Fill(null);
            }
        }
        #endregion
    }
}
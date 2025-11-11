using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Linq;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Compression of a sequence of nullable strings into a byte array.
    /// The byte array relies on the metadata carried by <see cref="SerializedColumn"/>.
    /// 
    /// There are two different encoding that are tryed and the one consuming the least
    /// amount of bytes is selected.
    /// 
    /// The first one encodes each string (<see cref="EncodeEachString(IEnumerable{string?})"/>),
    /// the second one encodes a dictionary of strings
    /// (<see cref="EncodeWithDictionary(IEnumerable{string?})"/> and then reference the
    /// entries in that dictionary in a sequence of int which
    /// is encoded with <see cref="Int64Codec"/>.
    /// 
    /// The first byte of the payload determines which encoding method.
    /// </summary>
    internal static class StringCodec
    {
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
        /// <param name="draftWriter"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static CompressedPackage<string?> Compress(
            IEnumerable<string?> values,
            ref ByteWriter writer,
            ByteWriter draftWriter)
        {
            if (values == null || !values.Any())
            {
                throw new ArgumentNullException(nameof(values));
            }

            var itemCount = values.Count();
            var uniqueValues = values
                .Where(v => v != null)
                .Select(v => v!)
                .Distinct()
                .OrderBy(v => v)
                .ToImmutableArray();

            if (itemCount > short.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(values), "Too many values");
            }
            if (uniqueValues.Any())
            {
                var revertDictionary = uniqueValues
                    .Index()
                    .ToImmutableDictionary(p => p.Item, p => (short)p.Index);
                var indexes = values
                    .Select(v => v == null ? -1 : revertDictionary[v])
                    .ToImmutableArray();
                var hasNulls = indexes.Any(v => v == -1);
                //  Transform the unique values into a sequence
                //  We punctuate the string by a null value
                var valuesSequence = uniqueValues
                    .Select(v => v.Select(c => (long?)Convert.ToInt16(c)).Append(null))
                    .SelectMany(s => s);

                //  Value sequence
                writer.WriteUInt16((ushort)(valuesSequence.Count()));

                var valueSequenceSizePlaceholder = writer.PlaceholderUInt16();
                var positionBeforeValueSequence = writer.Position;

                Int64Codec.Compress(valuesSequence, ref writer, draftWriter);
                valueSequenceSizePlaceholder.SetValue(
                    (ushort)(writer.Position - positionBeforeValueSequence));

                //  Indexes
                var indexesSizePlaceholder = writer.PlaceholderUInt16();
                var positionBeforeIndexes = writer.Position;

                BitPacker.Pack(
                    indexes.Select(i => (ulong)(i + 1)),
                    itemCount,
                    (ulong)(uniqueValues.Length),
                    ref writer);
                indexesSizePlaceholder.SetValue(
                    (ushort)(writer.Position - positionBeforeIndexes));

                return new(
                    itemCount,
                    hasNulls,
                    uniqueValues.First(),
                    uniqueValues.Last());
            }
            else
            {   //  Only nulls
                writer.WriteUInt16((ushort)0);

                return new(
                    itemCount,
                    true,
                    null,
                    null);
            }
        }
        #endregion

        #region Decompress
        public static IEnumerable<string?> Decompress(
            int itemCount,
            ReadOnlySpan<byte> payload)
        {
            IEnumerable<string> BreakStrings(IEnumerable<char?> valueSequence)
            {
                var charList = new List<char>();

                foreach (var value in valueSequence)
                {
                    if (value == null)
                    {
                        yield return new string(charList.ToArray());
                        charList.Clear();
                    }
                    else
                    {
                        charList.Add(value.Value);
                    }
                }
            }

            var payloadReader = new ByteReader(payload);
            var valuesSequenceCount = payloadReader.ReadUInt16();

            if (valuesSequenceCount > 0)
            {
                //  Value Sequence
                var valuesSequenceSize = payloadReader.ReadUInt16();
                var valuesSequence = Int64Codec.Decompress(
                    valuesSequenceCount,
                    true,
                    payloadReader.SpanForward(valuesSequenceSize))
                    .Select(l => (char?)l);
                //  Unique values
                var uniqueValues = BreakStrings(valuesSequence)
                    .ToImmutableArray();
                //  Indexes
                var indexSize = payloadReader.ReadUInt16();
                var unpackedIndexes = BitPacker.Unpack(
                    payloadReader.SpanForward(indexSize),
                    itemCount,
                    (ulong)(uniqueValues.Length));
                var values = ImmutableArray<string?>.Empty.ToBuilder();

                foreach (var unpackedIndex in unpackedIndexes)
                {
                    var index = (int)unpackedIndex - 1;
                    var value = index >= 0 ? uniqueValues[index] : null;

                    values.Add(value);
                }

                return values.ToImmutable();
            }
            else
            {
                return Enumerable.Range(0, itemCount)
                    .Select(i => (string?)null);
            }
        }
        #endregion
    }
}
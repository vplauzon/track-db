﻿using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;
using System.Text.RegularExpressions;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
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
        #region Inner Types
        #endregion

        #region Compress
        /// <summary>
        /// <paramref name="values"/> is enumerated into multiple times.
        /// 
        /// The general payload format is the following:
        /// 
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
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static SerializedColumn Compress(IEnumerable<string?> values)
        {
            if (values == null || !values.Any())
            {
                throw new ArgumentNullException(nameof(values));
            }

            var uniqueValues = values
                .Where(v => v != null)
                .Select(v => v!)
                .Distinct()
                .OrderBy(v => v)
                .ToImmutableArray();

            if (uniqueValues.Length > short.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(values), "Too many values");
            }
            if (uniqueValues.Any())
            {
                var revertDictionary = uniqueValues
                    .Index()
                    .ToImmutableDictionary(p => p.Item, p => (short)p.Index);
                var indexes = values
                    .Select(v => v == null ? -1 : (long?)revertDictionary[v])
                    .ToImmutableArray();
                var indexColumn = Int64Codec.Compress(indexes);
                var hasNulls = indexes.Any(v => v == -1);

                if (uniqueValues.Length > 2)
                {   //  General case:  we publish the unique values
                    //  Transform the unique values (excluding first & last) into a sequence
                    //  We punctuate the string by a null value
                    var valuesSequence = uniqueValues.Skip(1).SkipLast(1)
                        .Select(v => v.Select(c => (long?)Convert.ToInt16(c)).Append(null))
                        .SelectMany(s => s);
                    var valuesSequenceColumn = Int64Codec.Compress(valuesSequence);
                    var encodingMemory = EncodingMemory.FromValues(
                        (short)uniqueValues.Length,
                        (short)((long)valuesSequenceColumn.ColumnMinimum!),
                        (short)((long)valuesSequenceColumn.ColumnMaximum!),
                        (short)(valuesSequenceColumn.Payload.Length * sizeof(byte)),
                        (short)(valuesSequence.Count()),
                        valuesSequenceColumn.Payload,
                        (short)(indexColumn.Payload.Length * sizeof(byte)),
                        indexColumn.Payload);

                    return new SerializedColumn(
                        indexes.Length,
                        hasNulls,
                        uniqueValues.First(),
                        uniqueValues.Last(),
                        encodingMemory.Memory);
                }
                else if (uniqueValues.Length == 2 || hasNulls)
                {   //  2 unique values, can be deduced from column, but still need to serialize indexes
                    var encodingMemory = EncodingMemory.FromValues(
                        (short)uniqueValues.Length,
                        (short)(indexColumn.Payload.Length * sizeof(byte)),
                        indexColumn.Payload);

                    return new SerializedColumn(
                        indexes.Length,
                        hasNulls,
                        uniqueValues.First(),
                        uniqueValues.Last(),
                        encodingMemory.Memory);
                }
                else
                {   //  Unique repeated value
                    return new SerializedColumn(
                        indexes.Length,
                        hasNulls,
                        uniqueValues.First(),
                        uniqueValues.Last(),
                        Array.Empty<byte>());
                }
            }
            else
            {   //  Only nulls
                return new SerializedColumn(
                    values.Count(),
                    true,
                    null,
                    null,
                    Array.Empty<byte>());
            }
        }
        #endregion

        #region Decompress
        public static IEnumerable<string?> Decompress(SerializedColumn column)
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

            if (column.ColumnMinimum == null)
            {   //  All nulls
                return Enumerable.Range(0, column.ItemCount).Select(i => (string?)null);
            }
            else
            {
                var minValue = (string)column.ColumnMinimum!;
                var maxValue = (string)column.ColumnMaximum!;

                if (minValue == null && maxValue == null)
                {   //  Only nulls
                    return Enumerable.Range(0, column.ItemCount).Select(i => (string?)null);
                }
                else if (minValue == maxValue && !column.HasNulls)
                {   //  Unique value
                    return Enumerable.Range(0, column.ItemCount).Select(i => minValue);
                }
                else
                {
                    var encodingMemory = EncodingMemory.FromMemory(column.Payload);
                    var uniqueValuesCount = encodingMemory.ReadShort();

                    if (uniqueValuesCount > 2)
                    {   //  General case
                        var valuesSequenceMinimum = encodingMemory.ReadShort();
                        var valuesSequenceMaximum = encodingMemory.ReadShort();
                        var valuesSequencePayloadLength = encodingMemory.ReadShort();
                        var valuesSequenceLength = encodingMemory.ReadShort();
                        var valuesSequencePayload = encodingMemory.ReadArray(valuesSequencePayloadLength);
                        var valuesSequence = Int64Codec.Decompress(new SerializedColumn(
                            valuesSequenceLength,
                            true,
                            (long)valuesSequenceMinimum,
                            (long)valuesSequenceMaximum,
                            valuesSequencePayload));
                        var uniqueValues = BreakStrings(valuesSequence.Select(c => (char?)((short?)c)))
                            .Prepend(minValue)
                            .Append(maxValue)
                            .ToImmutableArray();
                        var indexColumnPayloadLength = encodingMemory.ReadShort();
                        var indexColumnPayload = encodingMemory.ReadArray(indexColumnPayloadLength);
                        var indexes = Int64Codec.Decompress(new SerializedColumn(
                            column.ItemCount,
                            false,
                            (long)(column.HasNulls ? -1 : 0),
                            (long)(uniqueValuesCount - 1),
                            indexColumnPayload));
                        var values = indexes
                            .Select(i => (short)i!)
                            .Select(i => i == -1 ? null : uniqueValues[i]);

                        return values;
                    }
                    else
                    {   //  2 or 1 unique values, can be deduced from column, but still need to serialize indexes
                        var indexColumnPayloadLength = encodingMemory.ReadShort();
                        var indexColumnPayload = encodingMemory.ReadArray(indexColumnPayloadLength);
                        var indexes = Int64Codec.Decompress(new SerializedColumn(
                            column.ItemCount,
                            false,
                            (long)(column.HasNulls ? -1 : 0),
                            (long)(uniqueValuesCount - 1),
                            indexColumnPayload));
                        var uniqueValues = new[] { minValue, maxValue };
                        var values = indexes
                            .Select(i => (short)i!)
                            .Select(i => i == -1 ? null : uniqueValues[i]);

                        return values;
                    }
                }
            }
        }
        #endregion
    }
}
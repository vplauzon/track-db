using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;

namespace TrackDb.Lib.Cache.CachedBlock.SpecializedColumn
{
    internal class ArrayIntColumn : PrimitiveArrayCachedColumnBase<int>
    {
        public ArrayIntColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override int NullValue => int.MinValue;

        protected override object? GetObjectData(int data)
        {
            return data == int.MinValue
                ? null
                : (object)data;
        }

        protected override void FilterBinaryInternal(
            int value,
            ReadOnlySpan<int> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<int>.Builder matchBuilder)
        {
            switch (binaryOperator)
            {
                case BinaryOperator.Equal:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] == value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThan:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] < value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThanOrEqual:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] <= value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                default:
                    throw new NotSupportedException(
                        $"{nameof(BinaryOperator)}:  '{binaryOperator}'");
            }
        }

        protected override SerializedColumn Serialize(ReadOnlyMemory<int> storedValues)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => v == NullValue ? null : (long?)v);
            var column = Int64Codec.Compress(values);

            //  Convert min and max to int-32 (from int-64)
            return new SerializedColumn(
                column.ItemCount,
                column.HasNulls,
                Convert.ToInt32(column.ColumnMinimum),
                Convert.ToInt32(column.ColumnMaximum),
                column.Payload);
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn serializedColumn)
        {
            //  Convert min and max to int-64 (from int-32)
            var intSerializedColumn = new SerializedColumn(
                serializedColumn.ItemCount,
                serializedColumn.HasNulls,
                Convert.ToInt64(serializedColumn.ColumnMinimum),
                Convert.ToInt64(serializedColumn.ColumnMaximum),
                serializedColumn.Payload);

            return Int64Codec.Decompress(intSerializedColumn)
                .Select(l => (int?)l)
                .Cast<object?>();
        }
    }
}
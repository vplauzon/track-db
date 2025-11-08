using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayShortColumn : PrimitiveArrayColumnBase<short>
    {
        public ArrayShortColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override short NullValue => short.MinValue;

        protected override object? GetObjectData(short data)
        {
            return data == short.MinValue
                ? null
                : (object)data;
        }

        protected override void FilterBinaryInternal(
            short value,
            ReadOnlySpan<short> storedValues,
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

        protected override SerializedColumn Serialize(ReadOnlyMemory<short> storedValues)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => v == NullValue ? null : (long?)v);
            var column = Int64Codec.Compress(values);

            //  Convert min and max to int-16 (from int-64)
            return new SerializedColumn(
                column.ItemCount,
                column.HasNulls,
                column.ColumnMinimum == null ? null : Convert.ToInt16(column.ColumnMinimum),
                column.ColumnMaximum == null ? null : Convert.ToInt16(column.ColumnMaximum),
                column.Payload);
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn column)
        {
            return Int64Codec.Decompress(column.ItemCount, column.HasNulls, column.Payload)
                .Select(l => (short?)l)
                .Cast<object?>();
        }
    }
}
using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayIntColumn : PrimitiveArrayColumnBase<int>
    {
        public ArrayIntColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override int NullValue => int.MinValue;

        protected override object? GetObjectData(int data)
        {
            return data == NullValue
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
                case BinaryOperator.NotEqual:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] != value)
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
                case BinaryOperator.GreaterThan:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] > value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.GreaterThanOrEqual:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] >= value)
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

        protected override int ComputeSerializationSizes(
            ReadOnlySpan<int> storedValues,
            Span<int> sizes,
            int maxSize)
        {
            var values = storedValues.Length <= 1024
                ? stackalloc long[storedValues.Length]
                : new long[storedValues.Length];

            for (var i = 0; i != storedValues.Length; ++i)
            {
                values[i] = storedValues[i];
            }

            return Int64Codec.ComputeSerializationSizes(values, NullValue, sizes, maxSize);
        }

        protected override ColumnStats Serialize(
            ReadOnlySpan<int> storedValues,
            ref ByteWriter writer)
        {
            var values = storedValues.Length <= 1024
                ? stackalloc long[storedValues.Length]
                : new long[storedValues.Length];

            for (var i = 0; i != storedValues.Length; ++i)
            {
                values[i] = storedValues[i];
            }

            var package = Int64Codec.Compress(values, NullValue, ref writer);

            //  Convert min and max to int-32 (from int-64)
            return new(
                package.ItemCount,
                package.HasNulls,
                package.ColumnMinimum == NullValue ? null : Convert.ToInt32(package.ColumnMinimum),
                package.ColumnMaximum == NullValue ? null : Convert.ToInt32(package.ColumnMaximum));
        }

        protected override void Deserialize(int itemCount, ReadOnlySpan<byte> payload)
        {
            IDataColumn dataColumn = this;
            var newValues = itemCount <= 1024
                ? stackalloc long[itemCount]
                : new long[itemCount];
            var payloadReader = new ByteReader(payload);

            Int64Codec.Decompress(ref payloadReader, newValues, NullValue);
            for (var i = 0; i != newValues.Length; ++i)
            {
                dataColumn.AppendValue((int)newValues[i]);
            }
        }
    }
}
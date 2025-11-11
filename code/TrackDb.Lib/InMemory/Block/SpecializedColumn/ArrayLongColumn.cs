using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayLongColumn : PrimitiveArrayColumnBase<long>
    {
        public ArrayLongColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override long NullValue => long.MinValue;

        protected override object? GetObjectData(long data)
        {
            return data == int.MinValue
                ? null
                : (object)data;
        }

        protected override void FilterBinaryInternal(
            long value,
            ReadOnlySpan<long> storedValues,
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

        protected override ColumnStats Serialize(
            ReadOnlyMemory<long> storedValues,
            ref ByteWriter writer,
            ByteWriter draftWriter)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => v == NullValue ? null : (long?)v);
            var package = Int64Codec.Compress(values, ref writer, draftWriter);

            //  No need to convert min and max
            return new(
                package.ItemCount,
                package.HasNulls,
                package.ColumnMinimum,
                package.ColumnMaximum);
        }

        protected override IEnumerable<object?> Deserialize(
            int itemCount,
            bool hasNulls,
            ReadOnlyMemory<byte> payload)
        {
            return Int64Codec.Decompress(itemCount, hasNulls, payload.Span)
                .Cast<object?>();
        }
    }
}
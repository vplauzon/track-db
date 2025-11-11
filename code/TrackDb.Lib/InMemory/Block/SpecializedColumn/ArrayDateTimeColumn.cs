using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayDateTimeColumn : PrimitiveArrayColumnBase<DateTime>
    {
        public ArrayDateTimeColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override DateTime NullValue => DateTime.MinValue;

        protected override object? GetObjectData(DateTime data)
        {
            return data == NullValue
                ? null
                : (object)data;
        }

        protected override void FilterBinaryInternal(
            DateTime value,
            ReadOnlySpan<DateTime> storedValues,
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

        protected override StatsSerializedColumn Serialize(
            ReadOnlyMemory<DateTime> storedValues,
            ref ByteWriter writer,
            ByteWriter draftWriter)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => v == NullValue ? null : (long?)v.Ticks);
            var package = Int64Codec.Compress(values, ref writer, draftWriter);

            //  Convert min and max to DateTime (from int-64)
            return new(
                package.ItemCount,
                package.HasNulls,
                package.ColumnMinimum == null
                ? null
                : new DateTime(((long?)package.ColumnMinimum)!.Value),
                package.ColumnMaximum == null
                ? null
                : new DateTime(((long?)package.ColumnMaximum)!.Value));
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn column)
        {
            return Int64Codec.Decompress(column.ItemCount, column.HasNulls, column.Payload.Span)
                .Select(l => l == null ? (DateTime?)null : new DateTime(l.Value))
                .Cast<object?>();
        }
    }
}
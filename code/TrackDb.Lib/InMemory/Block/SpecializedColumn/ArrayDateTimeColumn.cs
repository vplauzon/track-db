using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Linq;
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
            return data;
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

        protected override SerializedColumn Serialize(ReadOnlyMemory<DateTime> storedValues)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => v == NullValue ? null : (long?)v.Ticks);
            var column = Int64Codec.Compress(values);

            //  Convert min and max to DateTime (from int-64)
            return new SerializedColumn(
                column.ItemCount,
                column.HasNulls,
                column.ColumnMinimum == null
                ? null
                : new DateTime(((long?)column.ColumnMinimum)!.Value),
                column.ColumnMaximum == null
                ? null
                : new DateTime(((long?)column.ColumnMaximum)!.Value),
                column.Payload);
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn column)
        {
            //  Convert min and max to int-64 (from DateTime)
            var intSerializedColumn = new SerializedColumn(
                column.ItemCount,
                column.HasNulls,
                column.ColumnMinimum == null
                ? null
                : ((DateTime?)column.ColumnMinimum)!.Value.Ticks,
                column.ColumnMaximum == null
                ? null
                : ((DateTime?)column.ColumnMaximum)!.Value.Ticks,
                column.Payload);

            return Int64Codec.Decompress(intSerializedColumn)
                .Select(l => (int?)l)
                .Cast<object?>();
        }
    }
}
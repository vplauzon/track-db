using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayStringColumn : PrimitiveArrayColumnBase<string?>
    {
        public ArrayStringColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override string? NullValue => null;

        protected override object? GetObjectData(string? data)
        {
            return data;
        }

        protected override void FilterBinaryInternal(
            string? value,
            ReadOnlySpan<string?> storedValues,
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
                        if (storedValues[i]?.CompareTo(value) < 0)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThanOrEqual:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i]?.CompareTo(value) <= 0)
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

        protected override StatsSerializedColumn Serialize(ReadOnlyMemory<string?> storedValues)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i]);

            return StringCodec.Compress(values);
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn column)
        {
            return StringCodec.Decompress(column);
        }
    }
}
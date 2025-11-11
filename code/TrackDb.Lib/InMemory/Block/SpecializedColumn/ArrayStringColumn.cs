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

        protected override ColumnStats Serialize(
            ReadOnlyMemory<string?> storedValues,
            ref ByteWriter writer)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i]);
            var package = StringCodec.Compress(values, ref writer);

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
            return StringCodec.Decompress(itemCount, payload.Span);
        }
    }
}
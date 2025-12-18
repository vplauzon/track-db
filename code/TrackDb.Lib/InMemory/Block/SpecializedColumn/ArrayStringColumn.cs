using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
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

        protected override void ComputeSerializationSizes(
            ReadOnlySpan<string?> storedValues,
            Span<int> sizes,
            int maxSize)
        {
            StringCodec.ComputeSerializationSizes(storedValues, sizes, maxSize);
        }

        protected override ColumnStats Serialize(
            ReadOnlySpan<string?> storedValues,
            ref ByteWriter writer)
        {
            var package = StringCodec.Compress(storedValues, ref writer);

            return new(
                package.ItemCount,
                package.HasNulls,
                package.ColumnMinimum,
                package.ColumnMaximum);
        }

        protected override void Deserialize(int itemCount, ReadOnlySpan<byte> payload)
        {
            IDataColumn dataColumn = this;
            var payloadReader = new ByteReader(payload);
            var newValues = new string?[itemCount];

            StringCodec.Decompress(ref payloadReader, newValues);
            for (var i = 0; i != newValues.Length; ++i)
            {
                dataColumn.AppendValue(newValues[i]);
            }
        }
    }
}
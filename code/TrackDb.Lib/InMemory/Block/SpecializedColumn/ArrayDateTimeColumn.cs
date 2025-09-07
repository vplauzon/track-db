using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayDateTimeColumn : PrimitiveArrayCachedColumnBase<DateTime?>
    {
        public ArrayDateTimeColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override DateTime? NullValue => null;

        protected override object? GetObjectData(DateTime? data)
        {
            return data;
        }

        protected override void FilterBinaryInternal(
            DateTime? value,
            ReadOnlySpan<DateTime?> storedValues,
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

        protected override SerializedColumn Serialize(ReadOnlyMemory<DateTime?> storedValues)
        {
            throw new NotImplementedException();
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn serializedColumn)
        {
            throw new NotImplementedException();
        }
    }
}
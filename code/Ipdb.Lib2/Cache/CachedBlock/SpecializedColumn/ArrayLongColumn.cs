using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
{
    internal class ArrayLongColumn : PrimitiveArrayCachedColumnBase<long>
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

        protected override void FilterInternal(
            long value,
            ReadOnlySpan<long> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<short>.Builder matchBuilder)
        {
            switch (binaryOperator)
            {
                case BinaryOperator.Equal:
                    for (short i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] == value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.NotEqual:
                    for (short i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] != value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThan:
                    for (short i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] < value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThanOrEqual:
                    for (short i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] <= value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.GreaterThan:
                    for (short i = 0; i != storedValues.Length; ++i)
                    {
                        if (storedValues[i] > value)
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.GreaterThanOrEqual:
                    for (short i = 0; i != storedValues.Length; ++i)
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

        protected override SerializedColumn Serialize(ReadOnlyMemory<long> storedValues)
        {
            throw new NotImplementedException();
        }
    }
}
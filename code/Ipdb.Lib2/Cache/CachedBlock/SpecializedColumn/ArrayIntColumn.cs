using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
{
    internal class ArrayIntColumn : PrimitiveArrayCachedColumnBase<int>
    {
        public ArrayIntColumn(int capacity)
            : base(capacity)
        {
        }

        protected override int NullValue => int.MinValue;

        protected override object? GetObjectData(int data)
        {
            return data == int.MinValue
                ? null
                : (object)data;
        }

        protected override void FilterInternal(
            int value,
            ReadOnlySpan<int> storedValues,
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
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text.Json;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayEnumColumn<T> : PrimitiveArrayColumnBase<T>
        where T : struct, Enum
    {
        private readonly T _nullValue = (T)Enum.ToObject(typeof(T), -1);

        public ArrayEnumColumn(bool allowNull, int capacity)
            : base(allowNull, capacity)
        {
        }

        protected override T NullValue => _nullValue;

        protected override object? GetObjectData(T data)
        {
            return EqualityComparer<T>.Default.Equals(data, _nullValue)
                ? null
                : (object)data;
        }

        protected override void FilterBinaryInternal(
            T value,
            ReadOnlySpan<T> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<int>.Builder matchBuilder)
        {
            switch (binaryOperator)
            {
                case BinaryOperator.Equal:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (EqualityComparer<T>.Default.Equals(storedValues[i], value))
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThan:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (Convert.ToInt64(storedValues[i]) < Convert.ToInt64(value))
                        {
                            matchBuilder.Add(i);
                        }
                    }
                    return;
                case BinaryOperator.LessThanOrEqual:
                    for (int i = 0; i != storedValues.Length; ++i)
                    {
                        if (Convert.ToInt64(storedValues[i]) <= Convert.ToInt64(value))
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
            ReadOnlySpan<T> storedValues,
            Span<int> sizes,
            int maxSize)
        {
            var values = storedValues.Length <= 1024
                ? stackalloc long[storedValues.Length]
                : new long[storedValues.Length];

            for (var i = 0; i != storedValues.Length; ++i)
            {
                values[i] = Convert.ToInt64(storedValues[i]);
            }

            Int64Codec.ComputeSerializationSizes(
                values,
                Convert.ToInt64(NullValue),
                sizes,
                maxSize);
        }

        protected override ColumnStats Serialize(
            ReadOnlySpan<T> storedValues,
            ref ByteWriter writer)
        {
            var values = storedValues.Length <= 1024
                ? stackalloc long[storedValues.Length]
                : new long[storedValues.Length];

            for (var i = 0; i != storedValues.Length; ++i)
            {
                values[i] = Convert.ToInt64(storedValues[i]);
            }

            var package = Int64Codec.Compress(values, Convert.ToInt64(NullValue), ref writer);

            //  Convert min and max to int-16 (from int-64)
            return new(
                package.ItemCount,
                package.HasNulls,
                package.ColumnMinimum == Convert.ToInt64(NullValue) ? null : package.ColumnMinimum,
                package.ColumnMaximum == Convert.ToInt64(NullValue) ? null : package.ColumnMaximum);
        }

        protected override void Deserialize(int itemCount, ReadOnlySpan<byte> payload)
        {
            IDataColumn dataColumn = this;
            var newValues = itemCount <= 1024
                ? stackalloc long[itemCount]
                : new long[itemCount];
            var payloadReader = new ByteReader(payload);

            Int64Codec.Decompress(ref payloadReader, newValues, Convert.ToInt64(NullValue));
            for (var i = 0; i != newValues.Length; ++i)
            {
                dataColumn.AppendValue(Enum.ToObject(typeof(T), newValues[i]));
            }
        }

        protected override JsonElement GetLogValue(object? objectData)
        {
            var element = JsonSerializer.SerializeToElement(objectData?.ToString());

            return element;
        }

        protected override object? GetObjectDataFromLog(JsonElement logElement)
        {
            var text = JsonSerializer.Deserialize<string>(logElement);

            if (text == null)
            {
                return null;
            }
            else
            {
                if (Enum.TryParse<T>(text, out var enumValue))
                {
                    return enumValue;
                }
                else
                {
                    throw new InvalidDataException(
                        $"Can't parse value '{text}' for enum type {typeof(T).Name}");
                }
            }
        }
    }
}
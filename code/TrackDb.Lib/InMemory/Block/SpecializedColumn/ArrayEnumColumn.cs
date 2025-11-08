using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text.Json;
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

        protected override SerializedColumn Serialize(ReadOnlyMemory<T> storedValues)
        {
            var values = Enumerable.Range(0, storedValues.Length)
                .Select(i => storedValues.Span[i])
                .Select(v => EqualityComparer<T>.Default.Equals(v, _nullValue)
                ? (long?)null
                : Convert.ToInt64(v));
            var column = Int64Codec.Compress(values);

            //  Convert min and max to T (from int-64)
            return new SerializedColumn(
                column.ItemCount,
                column.HasNulls,
                column.ColumnMinimum == null
                ? null
                : Enum.ToObject(typeof(T), (long)column.ColumnMinimum!),
                column.ColumnMaximum == null
                ? null
                : Enum.ToObject(typeof(T), (long)column.ColumnMaximum!),
                column.Payload);
        }

        protected override IEnumerable<object?> Deserialize(SerializedColumn column)
        {
            //  Convert min and max to int-64 (from T)
            var intSerializedColumn = new LongCompressedPackage(
                column.ItemCount,
                column.HasNulls,
                column.ColumnMinimum == null
                ? null
                : Convert.ToInt64(column.ColumnMinimum!),
                column.ColumnMaximum == null
                ? null
                : Convert.ToInt64(column.ColumnMaximum!),
                column.Payload);

            return Int64Codec.Decompress(intSerializedColumn)
                .Select(l => l == null ? null : Enum.ToObject(typeof(T), l));
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
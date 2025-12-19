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
    internal class ArrayEnumColumn<T> : TransformProxyColumn
        where T : struct, Enum
    {
        public ArrayEnumColumn(bool allowNull, int capacity)
            : base(new ArrayLongColumn(allowNull, capacity))
        {
        }

        protected override object? OutToInValue(object? value)
        {
            var enumValue = (T?)value;
            var longValue = enumValue == null
                ? (long?)null
                : Convert.ToInt64(enumValue.Value);

            return longValue;
        }

        protected override object? InToOutValue(object? value)
        {
            var longValue = (long?)value;
            var enumValue = longValue == null
                ? (T?)null
                : Enum.ToObject(typeof(T), longValue.Value);

            return enumValue;
        }

        protected override JsonElement InToLogValue(object? value)
        {
            if (value == null)
            {
                return JsonSerializer.SerializeToElement<string?>(null);
            }
            else
            {
                var enumValue = (T)Enum.ToObject(typeof(T), value);

                return JsonSerializer.SerializeToElement(enumValue.ToString());
            }
        }

        protected override object? LogValueToIn(JsonElement logValue)
        {
            var text = JsonSerializer.Deserialize<string>(logValue);

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
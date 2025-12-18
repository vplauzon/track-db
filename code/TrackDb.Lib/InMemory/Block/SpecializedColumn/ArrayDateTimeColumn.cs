using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayDateTimeColumn : TransformProxyColumn
    {
        public ArrayDateTimeColumn(bool allowNull, int capacity)
            : base(new ArrayLongColumn(allowNull, capacity))
        {
        }

        protected override object? OutToInValue(object? value)
        {
            var dateTimeValue = (DateTime?)value;
            var longValue = dateTimeValue == null
                ? (long?)null
                : dateTimeValue.Value.Ticks;

            return longValue;
        }

        protected override object? InToOutValue(object? value)
        {
            var longValue = (long?)value;
            var dateTimeValue = longValue == null
                ? (DateTime?)null
                : new DateTime(longValue.Value);

            return dateTimeValue;
        }

        protected override JsonElement InToLogValue(object? value)
        {
            var dateTimeValue = (DateTime?)value;

            return JsonSerializer.SerializeToElement(dateTimeValue);
        }

        protected override object? LogValueToIn(JsonElement logValue)
        {
            return JsonSerializer.Deserialize<DateTime?>(logValue);
        }
    }
}
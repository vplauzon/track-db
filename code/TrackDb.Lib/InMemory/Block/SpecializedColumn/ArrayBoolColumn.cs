using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayBoolColumn : TransformProxyColumn
    {
        public ArrayBoolColumn(bool allowNull, int capacity)
            : base(new ArrayIntColumn(allowNull, capacity))
        {
        }

        protected override object? OutToInValue(object? value)
        {
            var boolValue = (bool?)value;
            var intValue = boolValue == null
                ? (int?)null
                : Convert.ToInt32(boolValue.Value);

            return intValue;
        }

        protected override object? InToOutValue(object? value)
        {
            var intValue = (int?)value;
            var boolValue = intValue == null
                ? (bool?)null
                : Convert.ToBoolean(intValue.Value);

            return boolValue;
        }

        protected override IInPredicate TransformPredicate(IInPredicate inPredicate)
        {
            var typedInPredicate = (InPredicate<bool>)inPredicate;

            return new InPredicate<int>(
                typedInPredicate.ColumnIndex,
                typedInPredicate.Values
                .Select(v => Convert.ToInt32(v))
                .ToHashSet(),
                typedInPredicate.IsIn);
        }

        protected override JsonElement InToLogValue(object? value)
        {
            var intValue = (int?)value;
            var boolValue = intValue == null
                ? (bool?)null
                : Convert.ToBoolean(intValue.Value);

            return JsonSerializer.SerializeToElement(boolValue);
        }

        protected override object? LogValueToIn(JsonElement logValue)
        {
            return JsonSerializer.Deserialize<bool?>(logValue);
        }
    }
}
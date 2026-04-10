using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class ArrayUriColumn : TransformProxyColumn
    {
        public ArrayUriColumn(bool allowNull, int capacity)
            : base(new ArrayStringColumn(allowNull, capacity))
        {
        }

        protected override object? OutToInValue(object? value)
        {
            var uriValue = (Uri?)value;
            var stringValue = uriValue == null
                ? null
                : uriValue.ToString();

            return stringValue;
        }

        protected override object? InToOutValue(object? value)
        {
            var stringValue = (string?)value;
            var uriValue = stringValue == null
                ? null
                : new Uri(stringValue);

            return uriValue;
        }

        protected override IInPredicate TransformPredicate(IInPredicate inPredicate)
        {
            var typedInPredicate = (InPredicate<Uri>)inPredicate;

            return new InPredicate<string>(
                typedInPredicate.ColumnIndex,
                typedInPredicate.Values
                .Select(v => v.ToString())
                .ToHashSet(),
                typedInPredicate.IsIn);
        }

        protected override JsonElement InToLogValue(object? value)
        {
            var stringValue = (string?)value;

            return JsonSerializer.SerializeToElement(stringValue);
        }

        protected override object? LogValueToIn(JsonElement logValue)
        {
            var text = JsonSerializer.Deserialize<string?>(logValue);

            return text == null ? null : new Uri(text);
        }
    }
}
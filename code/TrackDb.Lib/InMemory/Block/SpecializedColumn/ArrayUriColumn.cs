using System;
using System.Collections.Generic;
using System.Collections.Immutable;

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
    }
}
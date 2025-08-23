using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
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
                ? null
                : boolValue.Value
                ? (int?)1
                : 0;

            return intValue;
        }

        protected override object? InToOutValue(object? value)
        {
            var intValue = (int?)value;
            var boolValue = intValue == null
                ? null
                : intValue == 0
                ? (bool?)false
                : true;

            return boolValue;
        }
    }
}
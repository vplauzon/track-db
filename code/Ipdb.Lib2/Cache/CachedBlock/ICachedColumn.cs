using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface ICachedColumn
    {
        int RecordCount { get; }

        IImmutableList<object> Data { get; }

        IImmutableList<short> Filter(BinaryOperator binaryOperator, object? value);

        void AppendValue(object? value);
    }
}
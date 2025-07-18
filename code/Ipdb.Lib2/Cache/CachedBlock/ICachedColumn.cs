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

        IEnumerable<object?> Data { get; }

        IEnumerable<short> Filter(BinaryOperator binaryOperator, object? value);

        void AppendValue(object? value);
    }
}
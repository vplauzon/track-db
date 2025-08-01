using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface IReadOnlyDataColumn
    {
        int RecordCount { get; }

        object? GetValue(short index);

        IEnumerable<short> Filter(BinaryOperator binaryOperator, object? value);
    }
}
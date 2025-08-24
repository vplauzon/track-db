using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Cache.CachedBlock
{
    internal interface IReadOnlyDataColumn
    {
        int RecordCount { get; }

        object? GetValue(int index);

        IEnumerable<int> Filter(BinaryOperator binaryOperator, object? value);
    }
}
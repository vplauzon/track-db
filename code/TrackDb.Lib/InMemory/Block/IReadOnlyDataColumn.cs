using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block
{
    internal interface IReadOnlyDataColumn
    {
        int RecordCount { get; }

        object? GetValue(int index);

        IEnumerable<int> FilterBinary(BinaryOperator binaryOperator, object? value);
        
        IEnumerable<int> FilterIn(IImmutableSet<object?> values);
    }
}
using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.InMemory.Block
{
    internal interface IReadOnlyDataColumn
    {
        int RecordCount { get; }

        object? GetValue(int index);

        IEnumerable<JsonElement> GetLogValues();

        IEnumerable<int> FilterBinary(BinaryOperator binaryOperator, object? value);

        IEnumerable<int> FilterIn(IImmutableSet<object?> values);

        /// <summary>Serialize a segment.</summary>
        /// <param name="writer"></param>
        /// <param name="skipRows"></param>
        /// <param name="takeRows"></param>
        /// <returns></returns>
        ColumnStats SerializeSegment(ref ByteWriter writer, int skipRows, int takeRows);
    }
}
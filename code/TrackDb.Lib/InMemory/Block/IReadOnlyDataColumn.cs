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

        /// <summary>
        /// Computes the incremental column serialization size taking one element at the time.
        /// </summary>
        /// <param name="sizes">Array to write the sizes in</param>
        /// <param name="skipRecords">Number of records to skip at the beginning</param>
        /// <param name="maxSize">Maximum size, after which the array isn't expected to be completed</param>
        /// <returns>Number of records actually input in <paramref name="sizes"/>.</returns>
        int ComputeSerializationSizes(Span<int> sizes, int skipRecords, int maxSize);

        /// <summary>Serialize a segment.</summary>
        /// <param name="writer"></param>
        /// <param name="skipRecords"></param>
        /// <param name="takeRows"></param>
        /// <returns></returns>
        ColumnStats SerializeSegment(ref ByteWriter writer, int skipRecords, int takeRows);
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block
{
    internal interface IDataColumn : IReadOnlyDataColumn
    {
        /// <summary>Append value at the end of the column.</summary>
        /// <param name="value"></param>
        void AppendValue(object? value);

        /// <summary>Append an entire column of values.</summary>
        /// <param name="column"></param>
        void AppendColumn(IDataColumn column);

        /// <summary>
        /// Append logged values at the end of the column.
        /// </summary>
        /// <param name="values"></param>
        void AppendLogValues(IEnumerable<JsonElement> values);

        void Reorder(IEnumerable<int> orderIndexes);

        /// <summary>Delete record indexes.</summary>
        /// <param name="recordIndexes"></param>
        void DeleteRecords(IEnumerable<int> recordIndexes);
        
        /// <summary>Clear all data from column.</summary>
        void Clear();

        /// <summary>Deserialize the payload and insert records in the column.</summary>
        /// <param name="itemCount"></param>
        /// <paramref name="payload"></param>
        void Deserialize(int itemCount, ReadOnlySpan<byte> payload);
    }
}
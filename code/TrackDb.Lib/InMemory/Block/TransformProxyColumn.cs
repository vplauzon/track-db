using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Column proxying another column by transforming values.
    /// </summary>
    internal abstract class TransformProxyColumn : IDataColumn
    {
        private readonly IDataColumn _innerColumn;

        public TransformProxyColumn(IDataColumn innerColumn)
        {
            _innerColumn = innerColumn;
        }

        protected abstract object? OutToInValue(object? value);

        protected abstract object? InToOutValue(object? value);

        protected abstract JsonElement InToLogValue(object? value);

        protected abstract object? LogValueToIn(JsonElement logValue);

        #region IReadOnlyDataColumn
        int IReadOnlyDataColumn.RecordCount => _innerColumn.RecordCount;

        object? IReadOnlyDataColumn.GetValue(int index)
        {
            var inValue = _innerColumn.GetValue(index);

            return InToOutValue(inValue);
        }

        IEnumerable<JsonElement> IReadOnlyDataColumn.GetLogValues()
        {
            return Enumerable.Range(0, _innerColumn.RecordCount)
                .Select(i => InToLogValue(_innerColumn.GetValue(i)));
        }

        IEnumerable<int> IReadOnlyDataColumn.FilterBinary(
            BinaryOperator binaryOperator,
            object? value)
        {
            return _innerColumn.FilterBinary(binaryOperator, OutToInValue(value));
        }

        IEnumerable<int> IReadOnlyDataColumn.FilterIn(IImmutableSet<object?> values)
        {
            return _innerColumn.FilterIn(
                values
                .Select(o => OutToInValue(o))
                .ToImmutableHashSet());
        }

        void IReadOnlyDataColumn.ComputeSerializationSizes(
            Span<int> sizes,
            int skipRecords,
            int maxSize)
        {
            _innerColumn.ComputeSerializationSizes(sizes, skipRecords, maxSize);
        }

        ColumnStats IReadOnlyDataColumn.SerializeSegment(
            ref ByteWriter writer,
            int skipRows,
            int takeRows)
        {
            var package = _innerColumn.SerializeSegment(ref writer, skipRows, takeRows);

            return new(
                package.ItemCount,
                package.HasNulls,
                InToOutValue(package.ColumnMinimum),
                InToOutValue(package.ColumnMaximum));
        }
        #endregion

        #region IDataColumn
        void IDataColumn.AppendValue(object? value)
        {
            _innerColumn.AppendValue(OutToInValue(value));
        }

        void IDataColumn.AppendLogValues(IEnumerable<JsonElement> values)
        {
            foreach (var logValue in values)
            {
                _innerColumn.AppendValue(OutToInValue(LogValueToIn(logValue)));
            }
        }

        void IDataColumn.Reorder(IEnumerable<int> orderIndexes)
        {
            _innerColumn.Reorder(orderIndexes);
        }

        void IDataColumn.DeleteRecords(IEnumerable<int> recordIndexes)
        {
            _innerColumn.DeleteRecords(recordIndexes);
        }

        void IDataColumn.Deserialize(int itemCount, ReadOnlySpan<byte> payload)
        {
            _innerColumn.Deserialize(itemCount, payload);
        }
        #endregion
    }
}
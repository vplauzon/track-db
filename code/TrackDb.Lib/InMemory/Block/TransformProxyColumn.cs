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
            return InToOutValue(_innerColumn.GetValue(index));
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

        ColumnStats IReadOnlyDataColumn.Serialize(
            int? rowCount,
            ref ByteWriter writer,
            ByteWriter draftWriter)
        {
            var package = _innerColumn.Serialize(rowCount, ref writer, draftWriter);

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

        void IDataColumn.Deserialize(int itemCount, bool hasNulls, ReadOnlyMemory<byte> payload)
        {
            _innerColumn.Deserialize(itemCount, hasNulls, payload);
        }
        #endregion
    }
}
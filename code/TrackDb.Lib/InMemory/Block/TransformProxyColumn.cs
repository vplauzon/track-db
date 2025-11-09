using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
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

        StatsSerializedColumn IReadOnlyDataColumn.Serialize(int? rowCount)
        {
            var innerColumn = _innerColumn.Serialize(rowCount);

            return new(
                innerColumn.ItemCount,
                innerColumn.HasNulls,
                InToOutValue(innerColumn.ColumnMinimum),
                InToOutValue(innerColumn.ColumnMaximum),
                innerColumn.Payload);
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

        void IDataColumn.Deserialize(SerializedColumn serializedColumn)
        {
            var innerSerializedColumn = new SerializedColumn(
                serializedColumn.ItemCount,
                serializedColumn.HasNulls,
                serializedColumn.Payload);

            _innerColumn.Deserialize(innerSerializedColumn);
        }
        #endregion
    }
}
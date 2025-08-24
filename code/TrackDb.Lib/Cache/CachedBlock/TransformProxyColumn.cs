using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.Cache.CachedBlock
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

        int IReadOnlyDataColumn.RecordCount => _innerColumn.RecordCount;

        object? IReadOnlyDataColumn.GetValue(int index)
        {
            return InToOutValue(_innerColumn.GetValue(index));
        }

        IEnumerable<int> IReadOnlyDataColumn.Filter(BinaryOperator binaryOperator, object? value)
        {
            return _innerColumn.Filter(binaryOperator, OutToInValue(value));
        }

        void IDataColumn.AppendValue(object? value)
        {
            _innerColumn.AppendValue(OutToInValue(value));
        }

        void IDataColumn.Reorder(IEnumerable<int> orderIndexes)
        {
            _innerColumn.Reorder(orderIndexes);
        }

        void IDataColumn.DeleteRecords(IEnumerable<int> recordIndexes)
        {
            _innerColumn.DeleteRecords(recordIndexes);
        }

        SerializedColumn IDataColumn.Serialize()
        {
            var innerColumn = _innerColumn.Serialize();

            return new SerializedColumn(
                innerColumn.ItemCount,
                innerColumn.HasNulls,
                InToOutValue(innerColumn.ColumnMinimum),
                InToOutValue(innerColumn.ColumnMaximum),
                innerColumn.Payload);
        }

        void IDataColumn.Deserialize(SerializedColumn serializedColumn)
        {
            var innerSerializedColumn = new SerializedColumn(
                serializedColumn.ItemCount,
                serializedColumn.HasNulls,
                OutToInValue(serializedColumn.ColumnMinimum),
                OutToInValue(serializedColumn.ColumnMaximum),
                serializedColumn.Payload);

            _innerColumn.Deserialize(innerSerializedColumn);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    /// <summary>
    /// Specialized column store to leverage vectorized operation at compilation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal abstract class PrimitiveArrayColumnBase<T> : IDataColumn, ITypedReadOnlyDataColumn<T>
    {
        private readonly int MIN_CAPACITY = 10;

        private T[] _array;
        private int _itemCount = 0;

        protected PrimitiveArrayColumnBase(bool allowNull, int capacity)
        {
            AllowNull = allowNull;
            _array = new T[Math.Min(MIN_CAPACITY, capacity)];
        }

        public ReadOnlySpan<T> RawData => new ReadOnlySpan<T>(_array, 0, _itemCount);

        #region IReadOnlyDataColumn
        int IReadOnlyDataColumn.RecordCount => _itemCount;

        object? IReadOnlyDataColumn.GetValue(int index)
        {
            if (index < 0 || index >= _itemCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return GetObjectData(_array[index]);
        }

        IEnumerable<JsonElement> IReadOnlyDataColumn.GetLogValues()
        {
            foreach (var item in _array.Take(_itemCount))
            {
                yield return GetLogValue(GetObjectData(item));
            }
        }

        IEnumerable<int> IReadOnlyDataColumn.FilterBinary(BinaryOperator binaryOperator, object? value)
        {
            if (value != null && value.GetType() != typeof(T))
            {
                throw new InvalidCastException(
                    $"Column type is '{typeof(T).Name}' while predicate type is" +
                    $" '{value.GetType().Name}'");
            }

            var strongTypeValue = value == null ? NullValue : (T)value;
            var matchBuilder = ImmutableArray<int>.Empty.ToBuilder();

            FilterBinaryInternal(
                strongTypeValue,
                new ReadOnlySpan<T>(_array, 0, _itemCount),
                binaryOperator,
                matchBuilder);

            return matchBuilder;
        }

        IEnumerable<int> IReadOnlyDataColumn.FilterIn(IImmutableSet<object?> values, bool isIn)
        {
            if (isIn)
            {
                var matchBuilder = ImmutableArray<int>.Empty.ToBuilder();

                for (var i = 0; i != _itemCount; ++i)
                {
                    if (values.Contains(_array[i]))
                    {
                        matchBuilder.Add(i);
                    }
                }

                return matchBuilder.ToImmutable();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        int IReadOnlyDataColumn.ComputeSerializationSizes(
            Span<int> sizes,
            int skipRecords,
            int maxSize)
        {
            return ComputeSerializationSizes(
                _array
                .AsSpan()
                .Slice(skipRecords, Math.Min(_itemCount - skipRecords, sizes.Length)),
                sizes,
                maxSize);
        }

        ColumnStats IReadOnlyDataColumn.SerializeSegment(
            ref ByteWriter writer,
            int skipRecords,
            int takeRows)
        {
            if (_itemCount == 0)
            {
                throw new InvalidOperationException(
                    "Can't serialize as there are no items in data column");
            }
            if (skipRecords < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(skipRecords));
            }
            if (takeRows < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(takeRows));
            }
            if (skipRecords + takeRows > _itemCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(takeRows),
                    $"{skipRecords} + {takeRows} > {_itemCount}");
            }

            return Serialize(
                new ReadOnlySpan<T>(_array, skipRecords, takeRows),
                ref writer);
        }
        #endregion

        #region IDataColumn
        void IDataColumn.AppendValue(object? value)
        {
            var strongValue = GetPrimitiveData(value);

            if (_array.Length <= _itemCount)
            {
                var newArray = new T[Math.Max(MIN_CAPACITY, _array.Length * 2)];

                Array.Copy(_array, newArray, _array.Length);
                _array = newArray;
            }
            _array[_itemCount++] = strongValue;
        }

        void IDataColumn.AppendLogValues(IEnumerable<JsonElement> logValues)
        {
            IDataColumn column = this;

            foreach (var logValue in logValues)
            {
                var objectData = GetObjectDataFromLog(logValue);
                var primitiveData = GetPrimitiveData(objectData);

                column.AppendValue(primitiveData);
            }
        }

        void IDataColumn.Reorder(IEnumerable<int> orderIndexes)
        {
            if (_itemCount > 1)
            {
                var newArray = new T[_itemCount];
                var i = 0;

                foreach (var orderIndex in orderIndexes)
                {
                    if (i >= _itemCount)
                    {
                        throw new ArgumentOutOfRangeException(nameof(orderIndexes));
                    }
                    newArray[orderIndex] = _array[i];
                    ++i;
                }
                if (i < _itemCount)
                {
                    throw new ArgumentOutOfRangeException(nameof(orderIndexes));
                }
                _array = newArray;
            }
        }

        void IDataColumn.DeleteRecords(IEnumerable<int> recordIndexes)
        {
            int offset = 0;
            var recordIndexStack = new Stack<int>(recordIndexes.OrderDescending());

            for (var i = 0; i != _itemCount; ++i)
            {
                if (recordIndexStack.Any() && recordIndexStack.Peek() == i)
                {
                    ++offset;
                    recordIndexStack.Pop();
                }
                else if (offset != 0)
                {
                    _array[i - offset] = _array[i];
                }
            }
            _itemCount -= offset;
            if (_itemCount < _array.Length / 4 && _itemCount > MIN_CAPACITY)
            {
                var newArray = new T[Math.Max(MIN_CAPACITY, _itemCount * 2)];

                Array.Copy(_array, newArray, _itemCount);
                _array = newArray;
            }
        }

        void IDataColumn.Clear()
        {
            _itemCount = 0;
            if (_array.Length > 2 * MIN_CAPACITY)
            {
                _array = new T[MIN_CAPACITY];
            }
        }

        void IDataColumn.Deserialize(int itemCount, ReadOnlySpan<byte> payload)
        {
            Deserialize(itemCount, payload);
        }
        #endregion

        #region ITypedReadOnlyDataColumn
        ReadOnlySpan<T> ITypedReadOnlyDataColumn<T>.RecordValues => RawData;
        #endregion

        protected bool AllowNull { get; }

        protected abstract T NullValue { get; }

        protected abstract object? GetObjectData(T data);

        protected abstract void FilterBinaryInternal(
            T value,
            ReadOnlySpan<T> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<int>.Builder matchBuilder);

        protected abstract int ComputeSerializationSizes(
            ReadOnlySpan<T> storedValues,
            Span<int> sizes,
            int maxSize);

        protected abstract ColumnStats Serialize(
            ReadOnlySpan<T> storedValues,
            ref ByteWriter writer);

        protected abstract void Deserialize(int itemCount, ReadOnlySpan<byte> payload);

        protected virtual JsonElement GetLogValue(object? objectData)
        {
            return JsonSerializer.SerializeToElement(objectData);
        }

        protected virtual object? GetObjectDataFromLog(JsonElement logElement)
        {
            if (logElement.ValueKind == JsonValueKind.Null)
            {
                return null;
            }
            else
            {
                return JsonSerializer.Deserialize<T>(logElement);
            }
        }

        private T GetPrimitiveData(object? value)
        {
            return value == null
                ? (AllowNull ? NullValue : throw new ArgumentNullException(nameof(value)))
                : (T)value;
        }
    }
}
using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    /// <summary>
    /// Specialized column store to leverage vectorized operation at compilation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal abstract class PrimitiveArrayCachedColumnBase<T> : IDataColumn
    {
        private readonly int MIN_CAPACITY = 10;

        private T[] _array;
        private int _itemCount = 0;

        protected PrimitiveArrayCachedColumnBase(bool allowNull, int capacity)
        {
            AllowNull = allowNull;
            _array = new T[Math.Min(MIN_CAPACITY, capacity)];
        }

        public ReadOnlySpan<T> RawData => new ReadOnlySpan<T>(_array, 0, _itemCount);

        public IEnumerable<T> EnumerableRawData
        {
            get
            {
                for (var i = 0; i != _itemCount; ++i)
                {
                    yield return _array[i];
                }
            }
        }

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

        IEnumerable<int> IReadOnlyDataColumn.FilterIn(IImmutableSet<object?> values)
        {
            var matchBuilder = ImmutableArray<int>.Empty.ToBuilder();

            for (var i =0; i != _itemCount;++i)
            {
                if (values.Contains(_array[i]))
                {
                    matchBuilder.Add(i);
                }
            }

            return matchBuilder.ToImmutable();
        }
        #endregion

        #region IDataColumn
        void IDataColumn.AppendValue(object? value)
        {
            var strongValue = value == null
                ? (AllowNull ? NullValue : throw new ArgumentNullException(nameof(value)))
                : (T)value;

            if (_array.Length <= _itemCount)
            {
                var newArray = new T[Math.Max(MIN_CAPACITY, _array.Length * 2)];

                Array.Copy(_array, newArray, _array.Length);
                _array = newArray;
            }
            _array[_itemCount++] = strongValue;
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

        SerializedColumn IDataColumn.Serialize()
        {
            if (_itemCount == 0)
            {
                throw new InvalidOperationException(
                    "Can't serialize as there are no items in data column");
            }

            return Serialize(new ReadOnlyMemory<T>(_array, 0, _itemCount));
        }

        void IDataColumn.Deserialize(SerializedColumn serializedColumn)
        {
            IDataColumn dataColumn = this;
            var newValues = Deserialize(serializedColumn);

            foreach (var value in newValues)
            {
                dataColumn.AppendValue(value);
            }
        }
        #endregion

        protected bool AllowNull { get; }

        protected abstract T NullValue { get; }

        protected abstract object? GetObjectData(T data);

        protected abstract void FilterBinaryInternal(
            T value,
            ReadOnlySpan<T> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<int>.Builder matchBuilder);

        protected abstract SerializedColumn Serialize(ReadOnlyMemory<T> storedValues);

        protected abstract IEnumerable<object?> Deserialize(SerializedColumn serializedColumn);
    }
}
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
{
    /// <summary>
    /// Specialized column store to leverage vectorized operation at compilation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal abstract class PrimitiveArrayCachedColumnBase<T> : IDataColumn
    {
        private T[] _array;
        private int _itemCount = 0;

        protected PrimitiveArrayCachedColumnBase(bool allowNull, int capacity)
        {
            AllowNull = allowNull;
            _array = new T[Math.Min(10, capacity)];
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

        object? IReadOnlyDataColumn.GetValue(short index)
        {
            if (index < 0 || index >= _itemCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return _array[index];
        }

        IEnumerable<short> IReadOnlyDataColumn.Filter(BinaryOperator binaryOperator, object? value)
        {
            if (value != null && value.GetType() != typeof(T))
            {
                throw new InvalidCastException(
                    $"Column type is '{typeof(T).Name}' while predicate type is" +
                    $" '{value.GetType().Name}'");
            }

            var strongTypeValue = value == null ? NullValue : (T)value;
            var matchBuilder = ImmutableArray<short>.Empty.ToBuilder();

            FilterInternal(
                strongTypeValue,
                new ReadOnlySpan<T>(_array, 0, _itemCount),
                binaryOperator,
                matchBuilder);

            return matchBuilder;
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
                var newArray = new T[Math.Max(10, _array.Length * 2)];

                Array.Copy(_array, newArray, _array.Length);
                _array = newArray;
            }
            _array[_itemCount++] = strongValue;
        }

        void IDataColumn.DeleteRecords(IEnumerable<short> recordIndexes)
        {
            short offset = 0;
            var recordIndexStack = new Stack<short>(recordIndexes);

            for (short i = 0; i != _itemCount; ++i)
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
        }
        #endregion

        protected bool AllowNull { get; }

        protected abstract T NullValue { get; }

        protected abstract object? GetObjectData(T data);

        protected abstract void FilterInternal(
            T value,
            ReadOnlySpan<T> storedValues,
            BinaryOperator binaryOperator,
            ImmutableArray<short>.Builder matchBuilder);
    }
}
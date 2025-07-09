using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    internal class SimpleCachedColumn<T> : ICachedColumn
        where T : IEquatable<T>, IComparable<T>
    {
        private T[] _array;
        private int _itemCount = 0;

        public SimpleCachedColumn(IEnumerable<object> data)
        {
            _array = data.Cast<T>().ToImmutableArray().ToArray();
            _itemCount = _array.Length;
        }

        #region ICachedColumn
        int ICachedColumn.RecordCount => _itemCount;

        IEnumerable<object> ICachedColumn.Data =>
            _array.Take(_itemCount).Cast<object>().ToImmutableArray();

        void ICachedColumn.AppendValue(object? value)
        {
            if (value is T strongValue)
            {
                if (_array.Length <= _itemCount)
                {
                    var newArray = new T[Math.Max(10, _array.Length * 2)];

                    Array.Copy(_array, newArray, _array.Length);
                    _array = newArray;
                }
                _array[_itemCount++] = strongValue;
            }
            else
            {
                throw new ArgumentException(
                    $"Expected value type '{typeof(T).Name}' but got '{value?.GetType().Name}'");
            }
        }
        #endregion
    }
}
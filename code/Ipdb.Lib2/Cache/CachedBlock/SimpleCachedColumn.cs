using System;
using System.Collections.Generic;
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
        private T[] _array = new T[10];
        private int _itemCount = 0;

        void ICachedColumn.AppendValue(object? value)
        {
            if (value is T strongValue)
            {
                if (_array.Length <= _itemCount)
                {
                    var newArray = new T[_array.Length * 2];

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
    }
}
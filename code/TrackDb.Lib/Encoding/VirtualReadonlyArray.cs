using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    internal ref struct VirtualReadonlyArray<T>
    {
        private readonly ReadOnlySpan<byte> _span;
        private readonly int _arrayLength;
        private readonly Func<ReadOnlySpan<byte>, int, T> _extractor;

        public VirtualReadonlyArray(
            ReadOnlySpan<byte> span,
            int arrayLength,
            Func<ReadOnlySpan<byte>, int, T> extractor)
        {
            _span = span;
            _arrayLength = arrayLength;
            _extractor = extractor;
        }

        public T GetValue(int index)
        {
            if (index > _arrayLength)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            
            return _extractor(_span, index);
        }
    }
}
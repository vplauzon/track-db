using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Storage
{
    internal ref struct UnpackedValues
    {
        #region Inner Types
        public ref struct UnpackedValuesEnumerator
        {
            private readonly ReadOnlySpan<byte> _packed;
            private readonly int _bitsPerValue;
            private readonly int _itemCount;
            private int _index = 0;
            private int _currentBitPosition = 0;

            public UnpackedValuesEnumerator(ReadOnlySpan<byte> packed, int bitsPerValue, int itemCount)
            {
                _packed = packed;
                _bitsPerValue = bitsPerValue;
                _itemCount = itemCount;
            }

            public ulong Current { get; private set; } = 0;

            public bool MoveNext()
            {
                if (_index < _itemCount)
                {
                    // Calculate which byte we start reading from
                    var startByteIndex = _currentBitPosition / 8;
                    var bitOffsetInStartByte = _currentBitPosition % 8;
                    var remainingBits = _bitsPerValue;
                    var value = 0UL;
                    var bitsProcessed = 0;

                    while (remainingBits > 0)
                    {
                        // Calculate how many bits we can read from current byte
                        var bitsToRead = Math.Min(8 - bitOffsetInStartByte, remainingBits);

                        // Extract bits from current byte
                        var currentByte = _packed[startByteIndex];
                        var mask = bitsToRead == 64 ? ulong.MaxValue : (1UL << bitsToRead) - 1;
                        var bits = (currentByte >> bitOffsetInStartByte) & (byte)mask;

                        // Add these bits to our value (ensure 64-bit operations throughout)
                        value |= ((ulong)bits << bitsProcessed);

                        // Update our trackers
                        remainingBits -= bitsToRead;
                        bitsProcessed += bitsToRead;
                        startByteIndex++;
                        bitOffsetInStartByte = 0;
                    }
                    Current = value;
                    _currentBitPosition += _bitsPerValue;
                    ++_index;

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        #endregion

        private readonly ReadOnlySpan<byte> _packed;
        private readonly int _bitsPerValue;
        private readonly int _itemCount;

        public UnpackedValues(ReadOnlySpan<byte> packed, int bitsPerValue, int itemCount)
        {
            _packed = packed;
            _bitsPerValue = bitsPerValue;
            _itemCount = itemCount;
        }

        /// <summary>This is to support for-each.</summary>
        /// <returns></returns>
        public UnpackedValuesEnumerator GetEnumerator() => new(_packed, _bitsPerValue, _itemCount);

        public IImmutableList<T> ToImmutableArray<T>(Func<ulong, T> projection)
        {
            var builder = ImmutableArray<T>.Empty.ToBuilder();

            foreach (var item in this)
            {
                builder.Add(projection(item));
            }

            return builder.ToImmutableArray();
        }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Byte reader:  read bytes from a span with helper methods to deserialize primitive types.
    /// </summary>
    internal ref struct ByteReader
    {
        private readonly ReadOnlySpan<byte> _span;
        private int _position = 0;

        public ByteReader(ReadOnlySpan<byte> span)
        {
            _span = span;
        }

        #region Slicing
        public ReadOnlySpan<byte> SliceForward(int length)
        {
            var subSpan = _span.Slice(_position, length);

            _position += length;

            return subSpan;
        }

        public ByteReader SliceArrayUInt16(int count)
        {
            var length = sizeof(ushort) * count;
            var subSpan = SliceForward(length);

            return new(subSpan);
        }
        #endregion

        #region Read operations
        public byte ReadByte()
        {
            return _span[_position++];
        }

        public ushort ReadUInt16()
        {
            var size = sizeof(ushort);
            var subSpan = _span.Slice(_position);

            _position += size;

            return BinaryPrimitives.ReadUInt16LittleEndian(subSpan);
        }

        public long ReadInt64()
        {
            var size = sizeof(long);
            var subSpan = _span.Slice(_position);

            _position += size;

            return BinaryPrimitives.ReadInt64LittleEndian(subSpan);
        }
        #endregion
    }
}
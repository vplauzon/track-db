using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Byte writer:  writes bytes to a span with helper methods to serialize primitive types.
    /// </summary>
    internal ref struct ByteWriter
    {
        private readonly Span<byte> _span;
        private int _position = 0;

        public ByteWriter(Span<byte> buffer)
        {
            _span = buffer;
        }

        public int Position => _position;

        #region Slicing
        public Span<byte> SliceForward(int length)
        {
            var subSpan = _span.Slice(_position, length);

            _position += length;

            return subSpan;
        }

        public ByteWriter SliceArrayUInt16(int count)
        {
            var length = sizeof(ushort) * count;
            var subSpan = SliceForward(length);

            return new(subSpan);
        }
        #endregion

        #region Write operations
        public void WriteByte(byte value)
        {
            _span.Slice(_position)[0] = value;
            ++_position;
        }

        public void WriteInt16(short value)
        {
            //  According to ChatGpt, slightly more performant not to slice with the size
            //  as it does only one bound check instead of two
            var subSpan = _span.Slice(_position);

            BinaryPrimitives.WriteInt16LittleEndian(subSpan, value);
            _position += sizeof(short);
        }

        public void WriteUInt16(ushort value)
        {
            var subSpan = _span.Slice(_position);

            BinaryPrimitives.WriteUInt16LittleEndian(subSpan, value);
            _position += sizeof(ushort);
        }

        public void WriteInt64(long value)
        {
            var subSpan = _span.Slice(_position);

            BinaryPrimitives.WriteInt64LittleEndian(subSpan, value);
            _position += sizeof(long);
        }

        public void WriteBytes(ReadOnlySpan<byte> span)
        {
            var subSpan = _span.Slice(_position);

            span.Slice(0, span.Length).CopyTo(subSpan);
            _position += subSpan.Length;
        }
        #endregion
    }
}
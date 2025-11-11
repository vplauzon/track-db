using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Byte writer:  can write bytes different ways and can also emulate to write
    /// when no buffer (or not long enough buffer) is provided.
    /// </summary>
    internal ref struct ByteWriter
    {
        private readonly Span<byte> _span;
        private readonly bool _isStrict;

        /// <summary>
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="isStrict">
        /// Is strict about managing the size of the buffer.  <c>true</c> means it throws error when
        /// overflow.
        /// </param>
        public ByteWriter(Span<byte> buffer, bool isStrict)
        {
            _span = buffer;
            _isStrict = isStrict;
        }

        public int Position { get; private set; } = 0;

        public bool IsOverflow => Position > _span.Length;

        public byte[] ToArray()
        {
            if (IsOverflow)
            {
                throw new OverflowException();
            }

            return _span.Slice(0, Position).ToArray();
        }

        #region Copy
        public void CopyFrom(ReadOnlySpan<byte> source)
        {
            var destination = VirtualByteSpanForward(source.Length);

            destination.CopyFrom(source);
        }

        public void CopyFrom(VirtualByteSpan source)
        {
            var destination = VirtualByteSpanForward(source.Length);

            source.CopyTo(destination);
        }
        #endregion

        #region Into other objects
        public VirtualByteSpan VirtualByteSpanForward(int length)
        {
            var subSpan = SubSpanForward(length);

            return new(subSpan, length);
        }
        #endregion

        #region Placeholders
        #region Array
        public PlaceholderArrayWriter<ushort> PlaceholderArrayUInt16(int length)
        {
            var subSpan = SubSpanForward(length * sizeof(ushort));
            var placeholder = new PlaceholderArrayWriter<ushort>(
                subSpan,
                length,
                (span, i, value) => WriteUInt16ToSubSpan(
                    span.Slice(sizeof(ushort) * i, sizeof(ushort)),
                    value));

            return placeholder;
        }
        #endregion

        #region Simple Values
        public PlaceholderWriter<ushort> PlaceholderUInt16()
        {
            var subSpan = SubSpanForwardUInt16();
            var placeholder = new PlaceholderWriter<ushort>(
                subSpan,
                (span, value) => WriteUInt16ToSubSpan(span, value));

            return placeholder;
        }
        #endregion
        #endregion

        #region Write operations
        public void WriteInt16(short value)
        {
            var subSpan = SubSpanForwardInt16();

            WriteInt16ToSubSpan(subSpan, value);
        }

        private static void WriteInt16ToSubSpan(Span<byte> subSpan, short value)
        {
            if (subSpan.Length > 0)
            {
                BinaryPrimitives.WriteInt16LittleEndian(subSpan, value);
            }
        }
        private Span<byte> SubSpanForwardInt16()
        {
            return SubSpanForward(sizeof(short));
        }

        public void WriteUInt16(ushort value)
        {
            var subSpan = SubSpanForwardUInt16();

            WriteUInt16ToSubSpan(subSpan, value);
        }

        private static void WriteUInt16ToSubSpan(Span<byte> subSpan, ushort value)
        {
            if (subSpan.Length > 0)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(subSpan, value);
            }
        }

        private Span<byte> SubSpanForwardUInt16()
        {
            return SubSpanForward(sizeof(ushort));
        }

        public void WriteInt64(long value)
        {
            var subSpan = SubSpanForwardInt64();

            WriteInt64ToSubSpan(subSpan, value);
        }

        private static void WriteInt64ToSubSpan(Span<byte> subSpan, long value)
        {
            if (subSpan.Length > 0)
            {
                BinaryPrimitives.WriteInt64LittleEndian(subSpan, value);
            }
        }

        private Span<byte> SubSpanForwardInt64()
        {
            return SubSpanForward(sizeof(long));
        }

        public void WriteBytes(ReadOnlySpan<byte> span)
        {
            var subSpan = SubSpanForward(span.Length);

            if (subSpan.Length > 0)
            {
                span.CopyTo(subSpan);
            }
        }

        private Span<byte> SubSpanForward(int length)
        {
            if (Position + length <= _span.Length)
            {
                var span = _span.Slice(Position, length);

                Position += length;

                return span;
            }
            else if (!_isStrict)
            {
                Position += length;

                return new Span<byte>();
            }
            else
            {
                throw new OverflowException();
            }
        }
        #endregion
    }
}
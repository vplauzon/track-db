using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Storage
{
    /// <summary>
    /// Byte writer:  can write bytes different ways and can also emulate to write
    /// when no buffer (or not long enough buffer) is provided.
    /// </summary>
    internal ref struct ByteWriter
    {
        private readonly Span<byte> _span;

        public ByteWriter(Span<byte> buffer)
        {
            _span = buffer;
        }

        public int Position { get; private set; } = 0;

        public bool IsOverflow => Position > _span.Length;

        public byte[] ToArray()
        {
            return _span.ToArray();
        }

        public void CopyFrom(VirtualByteSpan source)
        {
            var destination = VirtualByteSpanForward(source.Length);

            source.CopyTo(destination);
        }

        #region Into other objects
        public VirtualByteSpan VirtualByteSpanForward(int length)
        {
            var byteSpan = Position + length <= _span.Length
                ? new VirtualByteSpan(_span.Slice(Position, length), length)
                : new VirtualByteSpan();

            Position += length;

            return byteSpan;
        }
        #endregion

        #region Placeholders
        public PlaceholderWriter<ushort> PlaceholderUInt16()
        {
            var subSpan = SubSpanForwardUInt16();
            var placeholder = new PlaceholderWriter<ushort>(
                subSpan,
                (span, value) => WriteUInt16ToSubSpan(span, value));

            return placeholder;
        }

        #endregion

        #region Write operations
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

        public void WriteBytes(ReadOnlySpan<byte> span)
        {
            var subSpan = SubSpanForward(span.Length);

            if (subSpan.Length > 0)
            {
                span.CopyTo(subSpan);
            }
        }
        #endregion

        #region Get sub spans
        private Span<byte> SubSpanForwardUInt16()
        {
            return SubSpanForward(sizeof(ushort));
        }

        private Span<byte> SubSpanForwardInt64()
        {
            return SubSpanForward(sizeof(long));
        }

        private Span<byte> SubSpanForward(int length)
        {
            var span = Position + length <= _span.Length
                ? _span.Slice(Position, length)
                : new Span<byte>();

            Position += length;

            return span;
        }
        #endregion
    }
}
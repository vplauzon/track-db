using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Byte reader:  can read bytes different ways.
    /// </summary>
    internal ref struct ByteReader
    {
        private readonly ReadOnlySpan<byte> _span;

        public ByteReader(ReadOnlySpan<byte> span)
        {
            _span = span;
        }

        public int Position { get; private set; } = 0;

        #region Into other objects
        public ReadOnlySpan<byte> SpanForward(int length)
        {
            if(Position + length > _span.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            var span = _span.Slice(Position, length);

            Position += length;

            return span;
        }

        public ReadOnlySpan<byte> RemainingSpanForward()
        {
            var span = _span.Slice(Position);

            Position += _span.Length;

            return span;
        }
        #endregion

        #region Read operations
        public ushort ReadUInt16()
        {
            var subSpan = SubSpanForwardUInt16();

            return BinaryPrimitives.ReadUInt16LittleEndian(subSpan);
        }

        public long ReadInt64()
        {
            var subSpan = SubSpanForwardInt64();

            return BinaryPrimitives.ReadInt64LittleEndian(subSpan);
        }
        #endregion

        #region Virtual Arrays
        public VirtualReadonlyArray<ushort> VirtualReadonlyArrayUInt16(int length)
        {
            var subSpan = SubSpanForward(length * sizeof(ushort));
            var virtualArray = new VirtualReadonlyArray<ushort>(
                subSpan,
                length,
                (span, i) =>
                {
                    if (span.Length > 0)
                    {
                        var subSpan = span.Slice(sizeof(ushort) * i, sizeof(ushort));
                        
                        return BinaryPrimitives.ReadUInt16LittleEndian(subSpan);
                    }
                    else
                    {
                        return 0;
                    }
                });

            return virtualArray;
        }
        #endregion


        #region Get sub spans
        private ReadOnlySpan<byte> SubSpanForwardUInt16()
        {
            return SubSpanForward(sizeof(ushort));
        }

        private ReadOnlySpan<byte> SubSpanForwardInt64()
        {
            return SubSpanForward(sizeof(long));
        }

        private ReadOnlySpan<byte> SubSpanForward(int length)
        {
            if (Position + length > _span.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            var span = _span.Slice(Position, length);

            Position += length;

            return span;
        }
        #endregion
    }
}
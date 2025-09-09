using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class EncodingMemory
    {
        private ReadOnlyMemory<byte> _remainingMemory;

        #region Constructors
        private EncodingMemory(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
            _remainingMemory = memory;
        }

        public static EncodingMemory FromMemory(ReadOnlyMemory<byte> memory)
        {
            return new EncodingMemory(memory);
        }

        public static EncodingMemory FromValues(params object[] values)
        {
            var size = values.Sum(v => GetEncodingSize(v));
            var payload = new byte[size];
            var remainingPayload = payload.AsMemory();

            foreach (var value in values)
            {
                remainingPayload = EncodeValue(value, remainingPayload);
            }

            return new EncodingMemory(payload);
        }
        #endregion

        public ReadOnlyMemory<byte> Memory { get; }

        #region Encoding
        private static Memory<byte> EncodeValue(object value, Memory<byte> payload)
        {
            var size = GetEncodingSize(value);
            var activePayload = payload.Slice(0, size);
            var remainingPayload = payload.Slice(size);

            switch (value)
            {
                case short v:
                    BinaryPrimitives.WriteInt16LittleEndian(activePayload.Span, v);
                    break;

                case int v:
                    BinaryPrimitives.WriteInt32LittleEndian(activePayload.Span, v);
                    break;

                case long v:
                    BinaryPrimitives.WriteInt64LittleEndian(activePayload.Span, v);
                    break;

                case byte[] byteArray:
                    byteArray.AsSpan().CopyTo(activePayload.Span);
                    break;

                case ReadOnlyMemory<byte> byteMemory:
                    byteMemory.Span.CopyTo(activePayload.Span);
                    break;

                default:
                    throw new NotSupportedException(
                        $"Type '{value.GetType().Name}' is "
                        + $"not supported for parameter '{nameof(value)}'");
            }

            return remainingPayload;
        }

        private static int GetEncodingSize(object value)
        {
            switch (value)
            {
                case short:
                    return sizeof(short);
                case int:
                    return sizeof(int);
                case long:
                    return sizeof(long);
                case byte[] byteArray:
                    return byteArray.Length * sizeof(byte);
                case ReadOnlyMemory<byte> byteMemory:
                    return byteMemory.Length * sizeof(byte);

                default:
                    throw new NotSupportedException(
                        $"Type '{value.GetType().Name}' is "
                        + $"not supported for parameter '{nameof(value)}'");
            }
        }
        #endregion

        #region Decoding
        public short ReadShort()
        {
            return Read(
                sizeof(short),
                payload => BinaryPrimitives.ReadInt16LittleEndian(payload.Span));
        }

        public int ReadInt()
        {
            return Read(
                sizeof(int),
                payload => BinaryPrimitives.ReadInt32LittleEndian(payload.Span));
        }

        public long ReadLong()
        {
            return Read(
                sizeof(long),
                payload => BinaryPrimitives.ReadInt64LittleEndian(payload.Span));
        }

        public ReadOnlyMemory<byte> ReadArray(int size)
        {
            return Read(
                size * sizeof(byte),
                payload => payload);
        }

        private T Read<T>(int size, Func<ReadOnlyMemory<byte>, T> valueExtract)
        {
            var activePayload = _remainingMemory.Slice(0, size);

            _remainingMemory = _remainingMemory.Slice(size);

            return valueExtract(activePayload);
        }
        #endregion
    }
}
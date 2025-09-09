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
        private readonly List<object> _values = new(20);

        public void Write(object value)
        {
            _values.Add(value);
        }

        public Memory<byte> Compile()
        {
            var size = _values.Sum(v => GetEncodingSize(v));
            var memory = new Memory<byte>(new byte[size]);
            var remainingMemory = memory;

            foreach (var value in _values)
            {
                remainingMemory = EncodeValue(value, remainingMemory);
            }
            if (remainingMemory.Length != 0)
            {
                throw new InvalidOperationException(
                    $"Remaing memory should be empty but still have " +
                    $"'{remainingMemory.Length}' bytes left");
            }

            return memory;
        }

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
    }
}
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block.SpecializedColumn
{
    internal class DecodingMemory
    {
        private ReadOnlyMemory<byte> _remainingMemory;

        public DecodingMemory(ReadOnlyMemory<byte> memory)
        {
            _remainingMemory = memory;
        }

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
    }
}
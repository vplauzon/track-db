using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.DbStorage
{
    internal class BlockWriter
    {
        private readonly List<byte> _buffer;
        private readonly Func<(int, MemoryMappedViewAccessor)> _blockReservation;

        public BlockWriter(
            int bufferMaxSize,
            Func<(int, MemoryMappedViewAccessor)> blockReservation)
        {
            _buffer = new List<byte>(bufferMaxSize);
            _blockReservation = blockReservation;
        }

        public Block ToBlock()
        {
            (var blockId, var accessor) = _blockReservation();

            using (accessor)
            {
                return new Block(blockId, (short)_buffer.Count);
            }
        }

        #region Write
        public void Write(short value)
        {
            _buffer.AddRange(BitConverter.GetBytes(value));
        }

        public void WriteArray<T>(T[] array) where T : notnull
        {
            if (typeof(T) == typeof(byte))
            {
                _buffer.AddRange(array.Cast<byte>());
            }
            else
            {
            }
        }
        #endregion
    }
}

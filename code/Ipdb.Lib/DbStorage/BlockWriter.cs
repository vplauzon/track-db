using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
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
            throw new NotImplementedException();
        }

        public void WriteArray<T>(T[] array)
        {
            throw new NotImplementedException();
        }

        public void Write(IEnumerable<byte> value)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
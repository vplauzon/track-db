using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;

namespace TrackDb.Lib.DbStorage
{
    internal class StorageManager : IDisposable
    {
        private const int BLOCK_SIZE = 4096;
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly string _filePath;
        private readonly MemoryMappedFile _mappedFile;
        private readonly Stack<int> _availableIds = new(
            Enumerable.Range(1, INCREMENT_BLOCK_COUNT + 1).Reverse());

        #region Constructors
        public StorageManager(string filePath)
        {
            _filePath = filePath;
            _mappedFile = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.Create,
                null,
                (long)INCREMENT_BLOCK_COUNT * BLOCK_SIZE);
        }
        #endregion

        public short BlockSize => BLOCK_SIZE;

        void IDisposable.Dispose()
        {
            _mappedFile.Dispose();
            File.Delete(_filePath);
        }

        public byte[] ReadBlock(int blockId)
        {
            using (var accessor = CreateViewAccessor(blockId, true))
            {
                var buffer = new byte[BLOCK_SIZE];

                accessor.ReadArray(0, buffer, 0, BLOCK_SIZE);

                return buffer;
            }
        }

        public int WriteBlock(byte[] buffer)
        {
            if (buffer.Length == 0 || buffer.Length > BLOCK_SIZE)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(buffer),
                    $"Buffer size:  {buffer.Length}");
            }

            var blockId = ReserveBlock();

            using (var accessor = CreateViewAccessor(blockId, false))
            {
                accessor.WriteArray(0, buffer, 0, buffer.Length);
            }

            return blockId;
        }

        public void ReleaseBlock(int blockId)
        {
            _availableIds.Push(blockId);
        }

        private MemoryMappedViewAccessor CreateViewAccessor(int blockId, bool isReadOnly)
        {
            return _mappedFile.CreateViewAccessor(
                blockId * BLOCK_SIZE,
                BLOCK_SIZE,
                isReadOnly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite);
        }

        private int ReserveBlock()
        {
            if (_availableIds.TryPop(out var blockId))
            {
                return blockId;
            }
            else
            {
                throw new NotImplementedException("Need to expend the file");
            }
        }
    }
}

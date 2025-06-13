using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Ipdb.Lib
{
    internal class StorageManager : IDisposable
    {
        private const int BLOCK_SIZE = 4096;
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly MemoryMappedFile _mappedFile;
        private readonly Stack<int> _availableIds = new();

        #region Constructors
        public StorageManager(string filePath)
        {
            _mappedFile = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.CreateNew,
                null,
                (long)INCREMENT_BLOCK_COUNT * BLOCK_SIZE);
            //  Push to the stack in reverse we start at the beginning of the file
            foreach (var blockId in Enumerable.Range(0, INCREMENT_BLOCK_COUNT).Reverse())
            {
                _availableIds.Push(blockId);
            }
        }
        #endregion

        public long BlockSize => BLOCK_SIZE;

        void IDisposable.Dispose()
        {
            _mappedFile.Dispose();
        }

        public int ReserveBlock()
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

        public void ReleaseBlock(int blockId)
        {
            _availableIds.Push(blockId);
        }

        public MemoryMappedViewAccessor CreateViewAccessor(int blockId, bool isReadOnly)
        {
            return _mappedFile.CreateViewAccessor(
                blockId * BLOCK_SIZE,
                BLOCK_SIZE,
                isReadOnly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite);
        }
    }
}

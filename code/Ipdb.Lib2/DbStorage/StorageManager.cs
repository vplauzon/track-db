using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;

namespace Ipdb.Lib2.DbStorage
{
    internal class StorageManager : IDisposable
    {
        private const int BLOCK_SIZE = 4096;
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly MemoryMappedFile _mappedFile;
        private readonly Stack<int> _availableIds = new(
            Enumerable.Range(0, INCREMENT_BLOCK_COUNT).Reverse());

        #region Constructors
        public StorageManager(string filePath)
        {
            _mappedFile = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.CreateNew,
                null,
                (long)INCREMENT_BLOCK_COUNT * BLOCK_SIZE);
        }
        #endregion

        public short BlockSize => BLOCK_SIZE;

        void IDisposable.Dispose()
        {
            _mappedFile.Dispose();
        }

        public BlockWriter GetBlockWriter()
        {
            return new BlockWriter(
                BlockSize,
                () =>
                {
                    var blockId = ReserveBlock();
                    var viewAccessor = CreateViewAccessor(blockId, false);

                    return (blockId, viewAccessor);
                });
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

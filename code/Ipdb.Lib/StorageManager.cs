using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;

namespace Ipdb.Lib
{
    internal class StorageManager : IDisposable
    {
        private const int EMPTY_LINE_CHAR_COUNT = 63;
        private const int EMPTY_LINE_COUNT = 64;
        private const long BLOCK_SIZE = (EMPTY_LINE_CHAR_COUNT + 1) * EMPTY_LINE_COUNT;
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
                INCREMENT_BLOCK_COUNT * BLOCK_SIZE);
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
            {   //  Initialize the block with spaces and carriage return
                using (var accessor = CreateViewAccessor(blockId, false))
                {
                    var blank = Enumerable.Range(0, EMPTY_LINE_COUNT)
                        .Select(i => Enumerable.Range(0, EMPTY_LINE_CHAR_COUNT).Select(j => ' '))
                        .Select(l => l.Append('\n'))
                        .SelectMany(c => c)
                        .ToArray();

                    if (blank.Length != BLOCK_SIZE)
                    {
                        throw new InvalidOperationException(
                            $"Blank block doesn't match with block size:  " +
                            $"{blank.Length} != {BLOCK_SIZE}");
                    }
                    accessor.WriteArray(0, blank, 0, blank.Length);
                }

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
                blockId * BLOCK_SIZE * sizeof(byte),
                BLOCK_SIZE * sizeof(byte),
                isReadOnly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite);
        }
    }
}
using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    /// <summary>
    /// Manages read / writes of blocks.
    /// Blocks start at index 1.
    /// </summary>
    internal class DatabaseFileManager : IDisposable
    {
        private readonly SafeFileHandle _fileHandle;
        private readonly string _filePath;
        private long _fileLength = 0;

        #region Constructors
        public DatabaseFileManager(string filePath, ushort blockSize)
        {
            _fileHandle = File.OpenHandle(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                FileOptions.RandomAccess);
            _filePath = filePath;
            BlockSize = blockSize;
        }
        #endregion

        void IDisposable.Dispose()
        {
            _fileHandle.Dispose();
            File.Delete(_filePath);
        }

        public ushort BlockSize { get; }

        #region Block read / write
        public byte[] ReadBlock(int blockId)
        {
            if (blockId < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(blockId));
            }
            var buffer = new byte[BlockSize];
            var position = GetBlockPosition(blockId);
            var readCount = RandomAccess.Read(_fileHandle, buffer, position);

            if (readCount != BlockSize)
            {
                throw new IOException(
                    $"Block read resulted in only {readCount} " +
                    $"bytes read instead of {BlockSize} ; file is {_fileLength} bytes long " +
                    $"and read started at {position}");
            }

            return buffer;
        }

        public void WriteBlock(int blockId, ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > BlockSize)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(buffer),
                    $"Buffer size:  {buffer.Length}");
            }

            var position = GetBlockPosition(blockId);

            RandomAccess.Write(_fileHandle, buffer, position);
        }

        private long GetBlockPosition(int blockId)
        {
            return (blockId - 1) * BlockSize;
        }
        #endregion

        public void EnsureBlockCapacity(int blockCount)
        {
            var requiredLength = blockCount * (long)BlockSize;
            var currentLength = RandomAccess.GetLength(_fileHandle);

            if (requiredLength > currentLength)
            {
                RandomAccess.SetLength(_fileHandle, requiredLength);
            }
        }
    }
}
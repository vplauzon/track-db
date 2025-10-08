using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;

namespace TrackDb.Lib
{
    internal class StorageManager : IDisposable
    {
        private const int BLOCK_SIZE = 4096;
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly string _filePath;
        private readonly ConcurrentStack<FileStream> _streamStack = new();

        #region Constructors
        public StorageManager(string filePath)
        {
            _filePath = filePath;
            //  Create the file itself
            ReleaseFileStream(CreateFileStream());
        }
        #endregion

        void IDisposable.Dispose()
        {
            foreach (var stream in _streamStack)
            {
                stream.Dispose();
            }
            File.Delete(_filePath);
        }

        public short BlockSize => BLOCK_SIZE;

        #region Block read / write
        public byte[] ReadBlock(int blockId)
        {
            var fileStream = AcquireFileStream();

            try
            {
                fileStream.Seek(GetBlockPosition(blockId), SeekOrigin.Begin);

                var buffer = new byte[BLOCK_SIZE];
                var readCount = fileStream.Read(buffer, 0, BLOCK_SIZE);

                if (readCount != BLOCK_SIZE)
                {
                    throw new IOException(
                        $"Block read resulted in only {readCount} " +
                        $"bytes read instead of {BLOCK_SIZE}");
                }

                return buffer;
            }
            finally
            {
                ReleaseFileStream(fileStream);
            }
        }

        public void WriteBlock(int blockId, ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > BLOCK_SIZE)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(buffer),
                    $"Buffer size:  {buffer.Length}");
            }

            var fileStream = AcquireFileStream();

            try
            {
                fileStream.Seek(GetBlockPosition(blockId), SeekOrigin.Begin);
                fileStream.Write(buffer);
            }
            finally
            {
                ReleaseFileStream(fileStream);
            }
        }

        private long GetBlockPosition(int blockId)
        {
            return (blockId - 1) * BLOCK_SIZE;
        }
        #endregion

        public IEnumerable<int> CreateBlockBatch()
        {
            var fileStream = AcquireFileStream();

            try
            {
                var currentBlockCount = (int)(fileStream.Length / BLOCK_SIZE);
                var targetBlockCount = currentBlockCount + INCREMENT_BLOCK_COUNT;

                fileStream.SetLength(targetBlockCount * (long)BLOCK_SIZE);

                return Enumerable.Range(currentBlockCount + 1, targetBlockCount);
            }
            finally
            {
                ReleaseFileStream(fileStream);
            }
        }

        #region File stream management
        private FileStream AcquireFileStream()
        {
            if (_streamStack.TryPop(out var fileStream))
            {
                return fileStream;
            }
            else
            {
                return CreateFileStream();
            }
        }

        private void ReleaseFileStream(FileStream fileStream)
        {
            _streamStack.Push(fileStream);
        }

        private FileStream CreateFileStream()
        {
            var fileStream = new FileStream(
               _filePath,
               FileMode.OpenOrCreate,
               FileAccess.ReadWrite,
               FileShare.ReadWrite,
               //  Disables FileStream’s internal buffer
               bufferSize: 1,
               FileOptions.RandomAccess | FileOptions.WriteThrough);

            return fileStream;
        }
        #endregion
    }
}
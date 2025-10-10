using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal class DbFileManager : IAsyncDisposable
    {
        private const int BLOCK_SIZE = 4096;
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly string _filePath;
        private readonly FileStream _fileStream;
        private readonly object _lock = new();

        #region Constructors
        public DbFileManager(string filePath)
        {
            _filePath = filePath;
            _fileStream = new FileStream(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                //  Disables FileStream’s internal buffer
                bufferSize: 1,
                FileOptions.RandomAccess | FileOptions.WriteThrough);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _fileStream.DisposeAsync();
            File.Delete(_filePath);
        }

        public short BlockSize => BLOCK_SIZE;

        #region Block read / write
        public byte[] ReadBlock(int blockId)
        {
            lock (_lock)
            {
                _fileStream.Seek(GetBlockPosition(blockId), SeekOrigin.Begin);

                var buffer = new byte[BLOCK_SIZE];
                var readCount = _fileStream.Read(buffer, 0, BLOCK_SIZE);

                if (readCount != BLOCK_SIZE)
                {
                    throw new IOException(
                        $"Block read resulted in only {readCount} " +
                        $"bytes read instead of {BLOCK_SIZE}");
                }

                return buffer;
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

            lock (_lock)
            {
                _fileStream.Seek(GetBlockPosition(blockId), SeekOrigin.Begin);
                _fileStream.Write(buffer);
            }
        }

        private long GetBlockPosition(int blockId)
        {
            return (blockId - 1) * BLOCK_SIZE;
        }
        #endregion

        public IEnumerable<int> CreateBlockBatch()
        {
            lock(_lock)
            {
                var currentBlockCount = (int)(_fileStream.Length / BLOCK_SIZE);
                var targetBlockCount = currentBlockCount + INCREMENT_BLOCK_COUNT;

                _fileStream.SetLength(targetBlockCount * (long)BLOCK_SIZE);

                return Enumerable.Range(currentBlockCount + 1, targetBlockCount);
            }
        }
    }
}
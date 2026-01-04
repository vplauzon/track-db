using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal class DatabaseFileManager : IAsyncDisposable
    {
        private const int INCREMENT_BLOCK_COUNT = 256;

        private readonly string _filePath;
        private readonly FileStream _fileStream;
        private readonly object _lock = new();
        private long _fileLength = 0;

        #region Constructors
        public DatabaseFileManager(string filePath, ushort blockSize)
        {
            _filePath = filePath;
            BlockSize = blockSize;
            _fileStream = new FileStream(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                //  Large buffer to allow async writes since local write aren't the real persistance layer
                bufferSize: 64 * 1024,
                FileOptions.RandomAccess);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _fileStream.DisposeAsync();
            File.Delete(_filePath);
        }

        public ushort BlockSize { get; }

        #region Block read / write
        public byte[] ReadBlock(int blockId)
        {
            lock (_lock)
            {
                var position = GetBlockPosition(blockId);

                _fileStream.Seek(GetBlockPosition(blockId), SeekOrigin.Begin);

                var buffer = new byte[BlockSize];
                var readCount = _fileStream.Read(buffer, 0, BlockSize);

                if (readCount != BlockSize)
                {
                    throw new IOException(
                        $"Block read resulted in only {readCount} " +
                        $"bytes read instead of {BlockSize} ; file is {_fileLength} bytes long " +
                        $"and read started at {position}");
                }

                return buffer;
            }
        }

        public void WriteBlock(int blockId, ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > BlockSize)
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
            return (blockId - 1) * BlockSize;
        }
        #endregion

        public IEnumerable<int> CreateBlockBatch()
        {
            lock (_lock)
            {
                var currentBlockCount = (int)(_fileStream.Length / BlockSize);
                var targetBlockCount = currentBlockCount + INCREMENT_BLOCK_COUNT;

                _fileStream.SetLength((targetBlockCount + 1) * (long)BlockSize);
                _fileLength = (targetBlockCount + 1) * (long)BlockSize;

                return Enumerable.Range(currentBlockCount + 1, INCREMENT_BLOCK_COUNT);
            }
        }
    }
}
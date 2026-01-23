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
    internal class DatabaseFileManager : IAsyncDisposable
    {
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
            if (blockId < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(blockId));
            }
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

        public void EnsureBlockCapacity(int blockCount)
        {
            lock (_lock)
            {
                var currentBlockCount = (int)(_fileStream.Length / BlockSize);

                if (blockCount > currentBlockCount)
                {
                    _fileStream.SetLength(blockCount * (long)BlockSize);
                    _fileLength = blockCount * (long)BlockSize;
                }
            }
        }
    }
}
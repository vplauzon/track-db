using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Ipdb.Lib.Document
{
    internal class DocumentManager : IDisposable
    {
        private const string DOCUMENTS_FILE_NAME = "documents.json";

        private readonly MemoryMappedFile _mappedFile;
        private long _nextOffset = 0;

        #region Constructor
        public DocumentManager(string databaseRootDirectory)
        {
            var filePath = Path.Combine(databaseRootDirectory, DOCUMENTS_FILE_NAME);

            Directory.CreateDirectory(databaseRootDirectory);
            _mappedFile = CreateOrOpenMemoryMappedFile(filePath);
        }

        private static MemoryMappedFile CreateOrOpenMemoryMappedFile(string filePath)
        {
            const long initialSize = 1024 * 1024; // 1MB

            return MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.OpenOrCreate,
                null,
                initialSize);
        }
        #endregion

        public long AppendDocument(byte[] metadata, byte[] document)
        {
            if (metadata == null || metadata.Length == 0)
            {
                throw new ArgumentException("Cannot be empty", nameof(metadata));
            }
            if (document == null || document.Length == 0)
            {
                throw new ArgumentException("Cannot be empty", nameof(document));
            }

            var startOffset = _nextOffset;

            using (var accessor = _mappedFile.CreateViewAccessor(
                _nextOffset,
                (sizeof(int) * 2) + metadata.Length + document.Length + 2))
            {
                var offset = 0;

                accessor.Write(offset, metadata.Length);
                offset += sizeof(int);
                accessor.Write(offset, document.Length);
                offset += sizeof(int);
                accessor.WriteArray(offset, metadata, 0, metadata.Length);
                offset += metadata.Length;
                accessor.Write(offset, (byte)'\n');
                offset += 1;
                accessor.WriteArray(offset, document, 0, document.Length);
                offset += document.Length;
                accessor.Write(offset, (byte)'\n');
                offset += 1;
                _nextOffset += offset;
            }

            return startOffset;
        }

        void IDisposable.Dispose()
        {
            if (_mappedFile != null)
            {
                _mappedFile.Dispose();
            }
        }
    }
}

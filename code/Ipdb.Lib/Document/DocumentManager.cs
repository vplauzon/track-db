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

        public long AppendDocument(byte[] document)
        {
            if (document == null)
            {
                throw new ArgumentNullException(nameof(document));
            }
            if (document.Length == 0)
            {
                throw new ArgumentException("Document cannot be empty", nameof(document));
            }

            using (var accessor = _mappedFile.CreateViewAccessor(_nextOffset, document.Length + 1))
            {
                accessor.WriteArray(0, document, 0, document.Length);
                accessor.Write(document.Length, (byte)'\n');
            }

            var documentOffset = _nextOffset;
            _nextOffset += document.Length + 1;

            return documentOffset;
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

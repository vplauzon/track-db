using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Ipdb.Lib.Document
{
    public class DocumentManager : IDisposable
    {
        private const string DOCUMENTS_FILE_NAME = "documents.json";

        private readonly MemoryMappedFile _mappedFile;

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

        void IDisposable.Dispose()
        {
            _mappedFile.Dispose();
        }
    }
}

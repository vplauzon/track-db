using System;
using System.Buffers;
using System.Text.Json;

namespace Ipdb.Lib.Document
{
    internal class DocumentManager : DataManagerBase
    {
        public DocumentManager(StorageManager storageManager)
            : base(storageManager)
        {
        }

        public FilePosition AppendDocument(int tableIndex, object document)
        {
            if (document == null)
            {
                throw new ArgumentNullException(nameof(document));
            }

            var serializedDocument = Serialize(document);

            if (serializedDocument.Length > StorageManager.BlockSize)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(document),
                    $"Document size:  {serializedDocument.Length}");
            }

            var blockId = StorageManager.ReserveBlock();

            using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
            {
                var startOffset = 0;
                var offset = startOffset;

                accessor.Write(offset, serializedDocument.Length * sizeof(byte));
                offset += sizeof(int);
                accessor.WriteArray(offset, serializedDocument, 0, serializedDocument.Length);
                offset += serializedDocument.Length * sizeof(byte);
                accessor.Write(offset, (byte)'\n');
                offset += 1;

                return new FilePosition(blockId, startOffset);
            }
        }
    }
}

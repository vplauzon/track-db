using Ipdb.Lib.Document;
using System;
using System.Reflection.Metadata;
using System.Xml.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class PrimaryIndexManager : DataManagerBase
    {
        public PrimaryIndexManager(StorageManager storageManager)
            :base(storageManager)
        {
        }

        public void AppendIndexes(int tableIndex, DocumentAllIndexes allIndexes)
        {
            if (allIndexes == null)
            {
                throw new ArgumentNullException(nameof(allIndexes));
            }

            var serializedAllIndexes = Serialize(allIndexes);

            if (serializedAllIndexes.Length > StorageManager.BlockSize)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(allIndexes),
                    $"All Indexes size:  {serializedAllIndexes.Length}");
            }

            var blockId = StorageManager.ReserveBlock();

            using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
            {
                var startOffset = 0;
                var offset = startOffset;

                accessor.Write(offset, serializedAllIndexes.Length * sizeof(byte));
                offset += sizeof(int);
                accessor.WriteArray(offset, serializedAllIndexes, 0, serializedAllIndexes.Length);
                offset += serializedAllIndexes.Length * sizeof(byte);
                accessor.Write(offset, (byte)'\n');
                offset += 1;
            }
        }
    }
}
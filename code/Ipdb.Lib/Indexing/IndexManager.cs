using Ipdb.Lib.Document;
using System;
using System.Collections.Immutable;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        public IndexManager(StorageManager storageManager)
            :base(storageManager)
        {
        }

        public void AppendIndex(
            int tableIndex,
            int indexIndex,
            short indexHash,
            long revisionId)
        {
            var blockId = StorageManager.ReserveBlock();

            using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
            {
                var startOffset = 0;
                var offset = startOffset;

                accessor.Write(offset, indexHash);
                offset += sizeof(short);
                accessor.Write(offset, revisionId);
                offset += sizeof(long);
            }
        }
    }
}
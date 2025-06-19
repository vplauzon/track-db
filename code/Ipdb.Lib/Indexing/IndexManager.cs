using Ipdb.Lib.Cache;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        public IndexManager(StorageManager storageManager)
            : base(storageManager)
        {
        }

        public void AppendIndex(
            string tableName,
            string propertyPath,
            short indexHash,
            long revisionId)
        {
            //var indexBlockCache =
            //    _indexBlockCacheMap[new TableIndexKey(tableName, propertyPath)];
            //var blocks = indexBlockCache.GetIndexBlocks(indexHash);

            //using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
            //{
            //    var startOffset = 0;
            //    var offset = startOffset;

            //    accessor.Write(offset, indexHash);
            //    offset += sizeof(short);
            //    accessor.Write(offset, revisionId);
            //    offset += sizeof(long);
            //}
        }

        public DatabaseCache? PersistIndexes(DatabaseCache cache, bool doPersistEverything)
        {
            throw new NotImplementedException();
        }
    }
}
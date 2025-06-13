using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        private readonly IImmutableDictionary<TableIndexKey, IndexBlockCache>
            _indexBlockCacheMap;

        #region Constructors
        public IndexManager(
            StorageManager storageManager,
            IImmutableList<TableIndexKey> tableIndexKeys)
            : base(storageManager)
        {
            _indexBlockCacheMap = tableIndexKeys
                .ToImmutableDictionary(k => k, k => new IndexBlockCache());
        }
        #endregion

        public void AppendIndex(
            string tableName,
            string propertyPath,
            short indexHash,
            long revisionId)
        {
            var indexBlockCache =
                _indexBlockCacheMap[new TableIndexKey(tableName, propertyPath)];
            var blockId = indexBlockCache.GetBlockId(indexHash);

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

        public IImmutableSet<long> FindEqualHash(
            string tableName,
            string propertyPath,
            short keyHash)
        {
            throw new NotImplementedException();
        }
    }
}
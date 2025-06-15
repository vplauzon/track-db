using Ipdb.Lib.Cache;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        private readonly IImmutableDictionary<TableIndexKey, IndexBlockCollection>
            _indexBlockCacheMap;

        #region Constructors
        public IndexManager(
            StorageManager storageManager,
            IImmutableList<TableIndexKey> tableIndexKeys)
            : base(storageManager)
        {
            _indexBlockCacheMap = tableIndexKeys
                .ToImmutableDictionary(k => k, k => new IndexBlockCollection());
        }
        #endregion

        #region Transaction
        public void OpenTransaction(long transactionId)
        {
            throw new NotImplementedException();
        }

        public void CompleteTransaction(long transactionId)
        {
            throw new NotImplementedException();
        }

        public void RollbackTransaction(long transactionId)
        {
            throw new NotImplementedException();
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
            var blocks = indexBlockCache.GetIndexBlocks(indexHash);

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

        public IImmutableSet<long> FindEqualHash(
            string tableName,
            string propertyPath,
            short keyHash)
        {
            throw new NotImplementedException();
        }
    }
}
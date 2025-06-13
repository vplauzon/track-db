using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        //private readonly IImmutableDictionary<TableIndexKey, IndexCache> _indexCaches;

        #region Constructors
        public IndexManager(
            StorageManager storageManager,
            DatabaseSchema databaseSchema)
            : base(storageManager)
        {
            //_indexCaches = tableSchemas
            //    .Select(p => new
            //    {
            //        TableName = p.Key,
            //        IndexMap = CreateIndexMap(p.Value)
            //    })
            //    .ToImmutableDictionary(o => o.TableName, o => o.IndexMap);
        }
        #endregion

        public void AppendIndex(
            string tableName,
            string propertyPath,
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

        public IImmutableSet<long> FindEqualHash(
            string tableName,
            string propertyPath,
            short keyHash)
        {
            throw new NotImplementedException();
        }
    }
}
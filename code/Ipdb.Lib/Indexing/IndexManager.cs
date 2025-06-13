using Ipdb.Lib.Document;
using System;
using System.Buffers;
using System.Collections.Immutable;
using System.Linq;
using System.Net.Http.Headers;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : DataManagerBase
    {
        private readonly IImmutableDictionary<string, IImmutableDictionary<string, IndexCache>>
            _blockMap;

        #region Constructors
        public IndexManager(
            StorageManager storageManager,
            IImmutableDictionary<string, IImmutableList<string>> tableIndexMap)
            : base(storageManager)
        {
            _blockMap = tableIndexMap
                .Select(p => new
                {
                    TableName = p.Key,
                    IndexMap = CreateIndexMap(p.Value)
                })
                .ToImmutableDictionary(o => o.TableName, o => o.IndexMap);
        }

        private static IImmutableDictionary<string, IndexCache> CreateIndexMap(
            IImmutableList<string> indexPropertyPaths)
        {
            return indexPropertyPaths
                .Select(path => new
                {
                    Path = path,
                    Cache = new IndexCache()
                })
                .ToImmutableDictionary(o => o.Path, o => o.Cache);
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
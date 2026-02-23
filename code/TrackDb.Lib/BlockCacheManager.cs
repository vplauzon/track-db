using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Text;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Policies;
using ZiggyCreatures.Caching.Fusion;

namespace TrackDb.Lib
{
    internal class BlockCacheManager : IDisposable
    {
        private readonly FusionCache _cache;
        private readonly Func<int, TableSchema, IBlock> _blockFactory;

        public BlockCacheManager(
            BlockCachePolicy blockCachePolicy,
            Func<int, TableSchema, IBlock> blockFactory)
        {
            var memoryCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = blockCachePolicy.BlockCount
            });

            _cache = new FusionCache(
                new FusionCacheOptions
                {
                    //  Default entry options, overridable per-call
                    DefaultEntryOptions = new FusionCacheEntryOptions
                    {
                        Duration = TimeSpan.MaxValue,   // we manage eviction manually
                        Priority = CacheItemPriority.Normal,
                        Size = 1
                    }
                },
                memoryCache);
            _blockFactory = blockFactory;
        }

        void IDisposable.Dispose()
        {
            _cache.Dispose();
        }

        public IBlock GetBlock(int blockId, TableSchema schema)
        {
            var priority = schema is MetadataTableSchema
                ? CacheItemPriority.High
                : CacheItemPriority.Normal;
            var options = new FusionCacheEntryOptions
            {
                Duration = TimeSpan.MaxValue,
                Priority = priority,
                Size = 1
            };
            var block = _cache.GetOrSet<IBlock>(
                blockId.ToString(),
                (ctx, ct) => _blockFactory(blockId, schema),
                options);

            return block;
        }

        public void InvalidateCache(int blockId)
        {
            _cache.Remove(blockId.ToString());
        }
    }
}
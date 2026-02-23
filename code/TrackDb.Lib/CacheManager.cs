using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Text;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib
{
    internal class CacheManager
    {
        private readonly IMemoryCache _memoryCache;

        public CacheManager(BlockCachePolicy blockCachePolicy)
        {
            _memoryCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = blockCachePolicy.BlockCount
            });
        }
    }
}
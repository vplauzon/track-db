using System;

namespace TrackDb.Lib.Policies
{
    public record BlockCachePolicy(int BlockCount)
    {
        public static BlockCachePolicy Create(int? BlockCount = null)
        {
            return new BlockCachePolicy(BlockCount ?? 50);
        }
    }
}
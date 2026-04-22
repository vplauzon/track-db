using System;

namespace TrackDb.Lib.Policies
{
    public record TombstonePolicy(
        TimeSpan FullBlockPeriod,
        TimeSpan PartialBlockPeriod,
        int PartialBlockRatio,
        TimeSpan TombstoneRetentionPeriod)
    {
        public static TombstonePolicy Create(
            TimeSpan? FullBlockPeriod = null,
            TimeSpan? PartialBlockPeriod = null,
            int? PartialBlockRatio = null,
            TimeSpan? TombstoneRetentionPeriod = null)
        {
            return new TombstonePolicy(
                 FullBlockPeriod ?? TimeSpan.FromSeconds(20),
                 PartialBlockPeriod ?? TimeSpan.FromMinutes(1),
                 PartialBlockRatio ?? 50,
                 PartialBlockPeriod ?? TimeSpan.FromMinutes(5));
        }
    }
}
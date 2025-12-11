using System;

namespace TrackDb.Lib.Policies
{
    public record LifeCyclePolicy(
        TimeSpan MaxWaitPeriod,
        TimeSpan BlockReleaseWaitPeriod)
    {
        public static LifeCyclePolicy Create(
            TimeSpan? MaxWaitPeriod = null,
            TimeSpan? BlockReleaseWaitPeriod = null)
        {
            return new LifeCyclePolicy(
                 MaxWaitPeriod ?? TimeSpan.FromSeconds(0.1),
                 BlockReleaseWaitPeriod ?? TimeSpan.FromSeconds(10));
        }
    }
}
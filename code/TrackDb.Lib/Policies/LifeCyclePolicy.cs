using System;

namespace TrackDb.Lib.Policies
{
    public record LifeCyclePolicy(
        TimeSpan MaxWaitPeriod)
    {
        public static LifeCyclePolicy Create(
            TimeSpan? MaxWaitPeriod = null)
        {
            return new LifeCyclePolicy(
                 MaxWaitPeriod ?? TimeSpan.FromSeconds(0.5));
        }
    }
}
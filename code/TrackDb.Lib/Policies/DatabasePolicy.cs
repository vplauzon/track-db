using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicy(
        LifeCyclePolicy LifeCyclePolicy,
        InMemoryPolicy InMemoryPolicy,
        StoragePolicy StoragePolicy,
        LogPolicy LogPolicy)
    {
        public static DatabasePolicy Create(
            LifeCyclePolicy? LifeCyclePolicy = null,
            InMemoryPolicy? InMemoryPolicy = null,
            StoragePolicy? StoragePolicy=null,
            LogPolicy? LogPolicy = null)
        {
            return new DatabasePolicy(
                LifeCyclePolicy?? LifeCyclePolicy.Create(),
                InMemoryPolicy ?? InMemoryPolicy.Create(),
				StoragePolicy ?? StoragePolicy.Create(),
                LogPolicy ?? LogPolicy.Create());
        }
    }
}
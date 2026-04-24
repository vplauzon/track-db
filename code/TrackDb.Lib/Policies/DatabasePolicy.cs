using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicy(
        LifeCyclePolicy LifeCyclePolicy,
        InMemoryPolicy InMemoryPolicy,
        TombstonePolicy TombstonePolicy,
        BlockCachePolicy BlockCachePolicy,
        StoragePolicy StoragePolicy,
        LogPolicy LogPolicy,
        DiagnosticPolicy DiagnosticPolicy)
    {
        public static DatabasePolicy Create(
            LifeCyclePolicy? LifeCyclePolicy = null,
            InMemoryPolicy? InMemoryPolicy = null,
            TombstonePolicy? TombstonePolicy = null,
            BlockCachePolicy? BlockCachePolicy=null,
            StoragePolicy? StoragePolicy = null,
            LogPolicy? LogPolicy = null,
            DiagnosticPolicy? DiagnosticPolicy = null)
        {
            return new DatabasePolicy(
                LifeCyclePolicy ?? LifeCyclePolicy.Create(),
                InMemoryPolicy ?? InMemoryPolicy.Create(),
                TombstonePolicy ?? TombstonePolicy.Create(),
                BlockCachePolicy ?? BlockCachePolicy.Create(),
                StoragePolicy ?? StoragePolicy.Create(),
                LogPolicy ?? LogPolicy.Create(),
                DiagnosticPolicy ?? DiagnosticPolicy.Create());
        }
    }
}
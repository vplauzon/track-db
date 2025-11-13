using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicy(
        InMemoryPolicy InMemoryPolicy,
        StoragePolicy StoragePolicy,
        LogPolicy LogPolicy)
    {
        public static DatabasePolicy Create(
            InMemoryPolicy? InMemoryPolicy = null,
            StoragePolicy? StoragePolicy=null,
            LogPolicy? LogPolicy = null)
        {
            return new DatabasePolicy(
                  InMemoryPolicy ?? InMemoryPolicy.Create(),
				  StoragePolicy ?? StoragePolicy.Create(),
                  LogPolicy ?? LogPolicy.Create());
        }
    }
}
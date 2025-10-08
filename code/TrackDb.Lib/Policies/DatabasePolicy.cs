using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicy(
        InMemoryPolicy InMemoryPolicy,
        LogPolicy LogPolicy)
    {
        public static DatabasePolicy Create(
            InMemoryPolicy? InMemoryPolicy = null,
            LogPolicy? LogPolicy = null)
        {
            return new DatabasePolicy(
                  InMemoryPolicy ?? InMemoryPolicy.Create(),
                  LogPolicy ?? LogPolicy.Create());
        }
    }
}
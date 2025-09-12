using System;

namespace TrackDb.Lib.Policies
{
    public record DatabasePolicies(
        InMemoryPolicies InMemoryPolicies)
    {
        public DatabasePolicies()
            : this(new InMemoryPolicies())
        {
        }
    }
}
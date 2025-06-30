using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache
{
    internal record ImmutableTableTransactionLog(
        ImmutableInMemoryBlock InMemoryBlock,
        IImmutableSet<long> DeletedRecordId);
}
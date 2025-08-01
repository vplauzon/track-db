using Ipdb.Lib2.Cache.CachedBlock;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache
{
    internal record ImmutableTableTransactionLog(
        IBlock InMemoryBlock,
        IImmutableSet<long> DeletedRecordIds);
}
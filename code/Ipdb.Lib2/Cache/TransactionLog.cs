using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache
{
    internal record TransactionLog(
        BlockBuilder BlockBuilder,
        ImmutableHashSet<long>.Builder DeletedRecordIds)
    {
        public TransactionLog()
            : this(new BlockBuilder(), ImmutableHashSet<long>.Empty.ToBuilder())
        {
        }

        public bool IsEmpty => BlockBuilder.IsEmpty && !DeletedRecordIds.Any();

        public ImmutableTransactionLog ToImmutable()
        {
            throw new NotImplementedException();
        }
    }
}
using TrackDb.Lib.Cache.CachedBlock;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

namespace TrackDb.Lib.Cache
{
    internal record ImmutableTableTransactionLogs(
        IImmutableList<IBlock> InMemoryBlocks)
    {
        public ImmutableTableTransactionLogs()
            : this(ImmutableArray<IBlock>.Empty)
        {
        }

        public ImmutableTableTransactionLogs(IBlock block)
            : this(new[] { block }.ToImmutableArray())
        {
        }

        public BlockBuilder MergeLogs()
        {
            if (!InMemoryBlocks.Any())
            {
                throw new InvalidOperationException("No in memory blocks to merge");
            }

            var newBlock = new BlockBuilder(InMemoryBlocks.First().TableSchema);

            foreach (var block in InMemoryBlocks)
            {
                newBlock.AppendBlock(block);
            }

            return newBlock;
        }
    }
}
using Ipdb.Lib2.Cache.CachedBlock;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

namespace Ipdb.Lib2.Cache
{
    internal record ImmutableTableTransactionLogs(
        IImmutableList<IBlock> InMemoryBlocks,
        Lazy<int> SerializedSize)
    {
        public ImmutableTableTransactionLogs()
            : this(ImmutableArray<IBlock>.Empty, new Lazy<int>(0))
        {
        }

        public ImmutableTableTransactionLogs(BlockBuilder blockBuilder)
            : this(
                  new[] { blockBuilder }.Cast<IBlock>().ToImmutableArray(),
                  new Lazy<int>(
                      () => blockBuilder.Serialize().Payload.Length,
                      LazyThreadSafetyMode.ExecutionAndPublication))
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
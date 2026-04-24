using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    internal class AvailabilityBlockManager
    {
        private const int BLOCK_INCREMENT = 256;

        private readonly Lazy<DatabaseFileManager> _dbFileManager;

        public AvailabilityBlockManager(Lazy<DatabaseFileManager> dbFileManager)
        {
            _dbFileManager = dbFileManager;
        }

        public IReadOnlyList<int> SetInUse(int blockIdCount, TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var blockIds = new int[blockIdCount];
            var i = 0;
            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;

            if (!blockIndex.TryGetValue(BlockAvailability.InUse, out var inUseBlockIndex))
            {
                inUseBlockIndex = new Dictionary<int, AvailableBlock>();
                blockIndex[BlockAvailability.InUse] = inUseBlockIndex;
            }
            while (i != blockIdCount)
            {
                if (blockIndex.TryGetValue(
                    BlockAvailability.Available,
                    out var availableBlockIndex)
                    && availableBlockIndex.Count > 0)
                {
                    while (i != blockIdCount && availableBlockIndex.Count > 0)
                    {
                        var availableBlock = availableBlockIndex.First().Value;

                        availableBlockIndex.Remove(availableBlock.BlockId);
                        inUseBlockIndex[availableBlock.BlockId] = availableBlock with
                        {
                            BlockAvailability = BlockAvailability.InUse,
                            VersionCount = availableBlock.VersionCount + 1
                        };
                        blockIds[i] = availableBlock.BlockId;
                        ++i;
                    }
                }
                else
                {
                    IncrementBlockCount(tx);
                }
            }
            ValidateValues(tx);

            return blockIds;
        }

        public void SetNoLongerInUse(IEnumerable<int> blockIds, TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var materializedBlockIds = blockIds.ToArray();
            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;

            if (!blockIndex.TryGetValue(BlockAvailability.InUse, out var inUseBlockIndex))
            {
                inUseBlockIndex = new Dictionary<int, AvailableBlock>();
                blockIndex[BlockAvailability.InUse] = inUseBlockIndex;
            }
            if (!blockIndex.TryGetValue(
                BlockAvailability.NoLongerInUse,
                out var noLongerInUseBlockIndex))
            {
                noLongerInUseBlockIndex = new Dictionary<int, AvailableBlock>();
                blockIndex[BlockAvailability.NoLongerInUse] = noLongerInUseBlockIndex;
            }

            foreach (var blockId in blockIds)
            {
                if (inUseBlockIndex.TryGetValue(blockId, out var block))
                {
                    inUseBlockIndex.Remove(blockId);
                    noLongerInUseBlockIndex[blockId] = block with
                    {
                        BlockAvailability = BlockAvailability.NoLongerInUse
                    };
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Block {blockId} is expected to be in use but not found");
                }
            }

            ValidateValues(tx);
        }

        public IEnumerable<int> ResetNoLongerInUsed(TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;

            if (blockIndex.TryGetValue(
                BlockAvailability.NoLongerInUse,
                out var noLongerInUseBlockIndex)
                && noLongerInUseBlockIndex.Count > 0)
            {
                var blockIds = noLongerInUseBlockIndex.Keys.ToArray();

                if (!blockIndex.TryGetValue(BlockAvailability.Available, out var availableBlockIndex))
                {
                    availableBlockIndex = new Dictionary<int, AvailableBlock>();
                    blockIndex[BlockAvailability.Available] = availableBlockIndex;
                }
                foreach (var block in noLongerInUseBlockIndex.Values.ToArray())
                {
                    noLongerInUseBlockIndex.Remove(block.BlockId);
                    availableBlockIndex[block.BlockId] = block with
                    {
                        BlockAvailability = BlockAvailability.Available
                    };
                }

                ValidateValues(tx);

                return blockIds;
            }
            else
            {
                return Array.Empty<int>();
            }
        }

        public int? GetBlockVersion(int blockId, TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;

            if (blockIndex.TryGetValue(BlockAvailability.InUse, out var inUseBlockIndex)
                && inUseBlockIndex.TryGetValue(blockId, out var block))
            {
                return block.VersionCount;
            }
            else
            {
                return null;
            }
        }

        private void IncrementBlockCount(TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;

            if (!blockIndex.TryGetValue(BlockAvailability.Available, out var availableBlockIndex))
            {
                availableBlockIndex = new Dictionary<int, AvailableBlock>();
                blockIndex[BlockAvailability.Available] = availableBlockIndex;
            }
            var allBlocks = blockIndex
                .Values
                .SelectMany(d => d.Values);
            var maxBlockId = allBlocks.Any()
                ? allBlocks.Max(a => a.BlockId)
                : 0;
            var targetCapacity = maxBlockId + BLOCK_INCREMENT;
            var newAvailableBlocks = Enumerable.Range(maxBlockId + 1, BLOCK_INCREMENT)
                .Select(id => new AvailableBlock(id, 0, BlockAvailability.Available));

            _dbFileManager.Value.EnsureBlockCapacity(targetCapacity);
            foreach (var block in newAvailableBlocks)
            {
                availableBlockIndex.Add(block.BlockId, block);
            }
        }

        [Conditional("DEBUG")]
        private void ValidateValues(TransactionContext tx)
        {
            tx.LoadAvailableBlockInTransaction();

            var blockIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingAvailableBlockIndex!;
            var allBlocks = blockIndex.Values
                .SelectMany(d => d.Values);

            if (allBlocks.Any())
            {
                //  Test for duplicates
                var duplicatedBlockIds = allBlocks
                    .CountBy(a => a.BlockId)
                    .Where(p => p.Value != 1)
                    .Select(p => p.Key)
                    .ToArray();

                if (duplicatedBlockIds.Length > 0)
                {
                    throw new InvalidOperationException(
                        $"{duplicatedBlockIds.Length} blocks are duplicated in availability table");
                }

                //  Test for missing blocks
                var minBlockId = allBlocks
                    .OrderBy(a => a.BlockId)
                    .Take(1)
                    .Select(a => a.BlockId)
                    .First();
                var maxBlockId = allBlocks
                    .OrderByDescending(a => a.BlockId)
                    .Take(1)
                    .Select(a => a.BlockId)
                    .First();
                var count = allBlocks.Count();

                if (minBlockId != 1)
                {
                    throw new InvalidOperationException(
                        $"MinBlockId is expected to be 1 not {minBlockId}");
                }
                if (maxBlockId != count)
                {
                    throw new InvalidOperationException(
                        $"MaxBlockId is expected to be {count} not {maxBlockId}");
                }
            }
        }
    }
}
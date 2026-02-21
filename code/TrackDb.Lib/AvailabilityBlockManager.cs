using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    internal class AvailabilityBlockManager
    {
        private const int BLOCK_INCREMENT = 256;

        private readonly TypedTable<AvailableBlockRecord> _availableBlockTable;
        private readonly Lazy<DatabaseFileManager> _dbFileManager;

        public AvailabilityBlockManager(
            TypedTable<AvailableBlockRecord> availableBlockTable,
            Lazy<DatabaseFileManager> dbFileManager)
        {
            _availableBlockTable = availableBlockTable;
            _dbFileManager = dbFileManager;
        }

        public IReadOnlyList<int> SetInUse(int blockIdCount, TransactionContext tx)
        {
            var blockIds = new List<int>(blockIdCount);

            while (blockIds.Count != blockIdCount)
            {
                var availableBlock = _availableBlockTable.Query(tx)
                    .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available))
                    .Take(1)
                    .FirstOrDefault();

                if (availableBlock == null)
                {
                    IncrementBlockCount(tx);
                }
                else
                {
                    var availableCount = availableBlock.MaxBlockId - availableBlock.MinBlockId + 1;
                    var takeBlockCount = Math.Min(blockIdCount - blockIds.Count, availableCount);

                    blockIds.AddRange(Enumerable.Range(availableBlock.MinBlockId, takeBlockCount));
                    _availableBlockTable.Query(tx)
                        .Where(pf => pf.Equal(a => a.MinBlockId, availableBlock.MinBlockId))
                        .Delete();
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            availableBlock.MinBlockId,
                            availableBlock.MinBlockId + takeBlockCount - 1,
                            BlockAvailability.InUse),
                        tx);
                    if (takeBlockCount < availableCount)
                    {
                        _availableBlockTable.AppendRecord(
                            new AvailableBlockRecord(
                                availableBlock.MinBlockId + takeBlockCount,
                                availableBlock.MaxBlockId,
                                BlockAvailability.Available),
                            tx);
                    }
                }
            }
            Merge(tx);
            ValidateValue(BlockAvailability.InUse, blockIds, tx);

            return blockIds;
        }

        public void SetNoLongerInUseBlockIds(IEnumerable<int> blockIds, TransactionContext tx)
        {
            void SetNoLongerInUseBlockId(int blockId)
            {
                var availableBlock = _availableBlockTable.Query(tx)
                    .Where(pf => pf.LessThanOrEqual(a => a.MinBlockId, blockId).And(
                        pf.GreaterThanOrEqual(a => a.MaxBlockId, blockId)))
                    .Take(1)
                    .FirstOrDefault();

                if (availableBlock == null)
                {
                    throw new ArgumentOutOfRangeException(nameof(blockId));
                }
                else if (availableBlock.BlockAvailability != BlockAvailability.InUse)
                {
                    throw new ArgumentException(
                        $"Expect {BlockAvailability.InUse.ToString()} but found " +
                        $"{availableBlock.BlockAvailability.ToString()}", nameof(blockId));
                }
                _availableBlockTable.Query(tx)
                    .Where(pf => pf.Equal(a => a.MinBlockId, availableBlock.MinBlockId))
                    .Delete();
                if (availableBlock.MinBlockId == blockId && availableBlock.MaxBlockId == blockId)
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                }
                else if (availableBlock.MinBlockId == blockId)
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            blockId + 1,
                            availableBlock.MaxBlockId,
                            BlockAvailability.InUse),
                        tx);
                }
                else if (availableBlock.MaxBlockId == blockId)
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            availableBlock.MinBlockId,
                            blockId - 1,
                            BlockAvailability.InUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                }
                else
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            availableBlock.MinBlockId,
                            blockId - 1,
                            BlockAvailability.InUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            blockId + 1,
                            availableBlock.MaxBlockId,
                            BlockAvailability.InUse),
                        tx);
                }
            }

            foreach (var blockId in blockIds)
            {
                SetNoLongerInUseBlockId(blockId);
            }
            Merge(tx);
            ValidateValue(BlockAvailability.NoLongerInUse, blockIds, tx);
        }

        public void ResetNoLongerInUsedBlocks(TransactionContext tx)
        {
            var noLongerInUsedBlocks = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUse))
                .ToImmutableArray();

            if (noLongerInUsedBlocks.Length > 0)
            {
                foreach (var noLongerInUsedBlock in noLongerInUsedBlocks)
                {
                    _availableBlockTable.Query(tx)
                        .Where(pf => pf.Equal(a => a.MinBlockId, noLongerInUsedBlock.MinBlockId))
                        .Delete();
                    _availableBlockTable.AppendRecord(
                        noLongerInUsedBlock with { BlockAvailability = BlockAvailability.Available },
                        tx);
                }
                Merge(tx);
#if DEBUG
                ValidateValue(
                    BlockAvailability.Available,
                    noLongerInUsedBlocks
                    .Select(b => Enumerable.Range(b.MinBlockId, b.MaxBlockId - b.MinBlockId + 1))
                    .SelectMany(id => id),
                    tx);
#endif
            }
        }

        private void IncrementBlockCount(TransactionContext tx)
        {
            var maxBlockId = _availableBlockTable.Query(tx)
                .OrderByDescending(a => a.MaxBlockId)
                .Take(1)
                .Select(a => a.MaxBlockId)
                .FirstOrDefault();
            var targetCapacity = maxBlockId + BLOCK_INCREMENT;
            var newAvailableBlock = new AvailableBlockRecord(
                maxBlockId + 1,
                maxBlockId + BLOCK_INCREMENT,
                BlockAvailability.Available);

            _dbFileManager.Value.EnsureBlockCapacity(targetCapacity);
            _availableBlockTable.AppendRecord(newAvailableBlock, tx);
        }

        private void Merge(TransactionContext tx)
        {
            var allBlocks = _availableBlockTable.Query(tx)
                .AsEnumerable()
                .OrderBy(a => a.MinBlockId)
                .ToImmutableList();

            Validate(allBlocks);
            if (allBlocks.Count > 1)
            {
                var beforeBlock = allBlocks[0];

                //  Clean all and rebuild
                _availableBlockTable.Query(tx)
                    .Delete();
                foreach (var block in allBlocks.Skip(1))
                {
                    if (beforeBlock.BlockAvailability == block.BlockAvailability)
                    {   //  Merge
                        beforeBlock = new AvailableBlockRecord(
                            beforeBlock.MinBlockId,
                            block.MaxBlockId,
                            beforeBlock.BlockAvailability);
                    }
                    else
                    {
                        _availableBlockTable.AppendRecord(beforeBlock, tx);
                        beforeBlock = block;
                    }
                }
                //  Re-append the last one
                _availableBlockTable.AppendRecord(beforeBlock, tx);
            }
        }

        [Conditional("DEBUG")]
        private void Validate(IImmutableList<AvailableBlockRecord> orderedBlocks)
        {
            void Validate(AvailableBlockRecord block)
            {
                if (block.MinBlockId > block.MaxBlockId)
                {
                    throw new InvalidOperationException("Min / Max out of order");
                }
            }
            foreach (var block in orderedBlocks)
            {
                Validate(orderedBlocks[0]);
            }
            if (orderedBlocks.Count > 0)
            {
                if (orderedBlocks[0].MinBlockId != 1)
                {
                    throw new InvalidOperationException(
                        $"MinBlockId should be 1 but is {orderedBlocks[0].MinBlockId}");
                }
            }
            if (orderedBlocks.Count > 1)
            {
                var beforeBlock = orderedBlocks[0];

                foreach (var block in orderedBlocks.Skip(1))
                {
                    if (beforeBlock.MaxBlockId + 1 != block.MinBlockId)
                    {
                        throw new InvalidOperationException("Max and min not contiguous");
                    }
                    beforeBlock = block;
                }
            }
        }

        [Conditional("DEBUG")]
        private void ValidateValue(
            BlockAvailability blockAvailability,
            IEnumerable<int> blockIds,
            TransactionContext tx)
        {
            foreach (var blockId in blockIds)
            {
                var availableBlockCount = _availableBlockTable.Query(tx)
                    .Where(pf => pf.LessThanOrEqual(a => a.MinBlockId, blockId).And(
                        pf.GreaterThanOrEqual(a => a.MaxBlockId, blockId)))
                    .Where(pf => pf.Equal(a => a.BlockAvailability, blockAvailability))
                    .Count();

                if (availableBlockCount != 1)
                {
                    throw new InvalidOperationException("Post condition invalid");
                }
            }
        }
    }
}
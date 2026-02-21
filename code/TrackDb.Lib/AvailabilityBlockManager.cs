using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

            return blockIds;
        }

        public void SetNoLongerInUseBlockIds(IEnumerable<int> blockIds, TransactionContext tx)
        {
            void SetNoLongerInUseBlockId(int blockId)
            {
                var availableBlock = _availableBlockTable.Query(tx)
                    .Where(pf => pf.GreaterThanOrEqual(a => a.MinBlockId, blockId).And(
                        pf.LessThanOrEqual(a => a.MaxBlockId, blockId)))
                    .Take(1)
                    .FirstOrDefault();

                if (availableBlock == null)
                {
                    throw new ArgumentOutOfRangeException(nameof(blockId));
                }
                else if (availableBlock.BlockAvailability == BlockAvailability.Available)
                {
                    throw new ArgumentException(
                        $"Expect {BlockAvailability.Available.ToString()} but found " +
                        $"{availableBlock.BlockAvailability.ToString()}", nameof(blockId));
                }
                _availableBlockTable.Query(tx)
                    .Where(pf => pf.Equal(a => a.MinBlockId, availableBlock.MinBlockId))
                    .Delete();
                if (availableBlock.MinBlockId == blockId)
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            blockId + 1,
                            availableBlock.MaxBlockId,
                            BlockAvailability.Available),
                        tx);
                }
                else if (availableBlock.MaxBlockId == blockId)
                {
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            availableBlock.MinBlockId,
                            blockId - 1,
                            BlockAvailability.Available),
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
                            BlockAvailability.Available),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(blockId, blockId, BlockAvailability.NoLongerInUse),
                        tx);
                    _availableBlockTable.AppendRecord(
                        new AvailableBlockRecord(
                            blockId + 1,
                            availableBlock.MaxBlockId,
                            BlockAvailability.Available),
                        tx);
                }
            }

            foreach (var blockId in blockIds)
            {
                SetNoLongerInUseBlockId(blockId);
            }
            Merge(tx);
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
                maxBlockId + 1 + BLOCK_INCREMENT,
                BlockAvailability.Available);

            _dbFileManager.Value.EnsureBlockCapacity(targetCapacity);
            _availableBlockTable.AppendRecord(newAvailableBlock, tx);
        }

        private void Merge(TransactionContext tx)
        {
            var allBlocks = _availableBlockTable.Query(tx)
               .OrderByDescending(a => a.MinBlockId)
               .ToImmutableList();

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
            }
        }
    }
}
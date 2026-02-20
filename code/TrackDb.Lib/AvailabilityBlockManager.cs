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

        public int UseAvailableBlockId(TransactionContext tx)
        {
            var blockIds = UseAvailableBlockIds(1, tx);

            return blockIds[0];
        }

        public IReadOnlyList<int> UseAvailableBlockIds(int blockIdCount, TransactionContext tx)
        {
            var availableBlockIds = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available))
                .Take(blockIdCount)
                .Select(a => a.BlockId)
                .ToImmutableArray();

            if (availableBlockIds.Length == blockIdCount)
            {
                var newRecords = availableBlockIds
                    .Select(id => new AvailableBlockRecord(id, BlockAvailability.InUsed));
                var deletedCount = _availableBlockTable.Query(tx)
                    .Where(pf => pf.In(a => a.BlockId, availableBlockIds))
                    .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available))
                    .Delete();

                if (deletedCount != availableBlockIds.Length)
                {
                    throw new InvalidOperationException($"Corrupted available blocks");
                }
                _availableBlockTable.AppendRecords(newRecords, tx);

                return availableBlockIds;
            }
            else
            {
                IncrementBlockCount(tx);

                //  Now that there are available block, let's try again
                return UseAvailableBlockIds(blockIdCount, tx);
            }
        }

        public void SetNoLongerInUsedBlockIds(IEnumerable<int> blockIds, TransactionContext tx)
        {
            var materializedBlockIds = blockIds.ToImmutableArray();
            var invalidBlockCount = _availableBlockTable.Query(tx)
                .Where(pf => pf.In(a => a.BlockId, materializedBlockIds))
                .Where(pf => pf.NotEqual(a => a.BlockAvailability, BlockAvailability.InUsed))
                .Count();

            if (invalidBlockCount > 0)
            {
                throw new InvalidOperationException($"{invalidBlockCount} invalid blocks, " +
                    $"i.e. not InUsed");
            }

            var deletedUsedBlocks = _availableBlockTable.Query(tx)
                .Where(pf => pf.In(a => a.BlockId, materializedBlockIds))
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.InUsed))
                .Delete();

            if (deletedUsedBlocks != materializedBlockIds.Count())
            {
                throw new InvalidOperationException(
                    $"Corrupted available blocks:  {materializedBlockIds.Count()} " +
                    $"to release from use, {deletedUsedBlocks} found");
            }
            _availableBlockTable.AppendRecords(
                materializedBlockIds
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.NoLongerInUsed)),
                tx);
        }

        public bool ReleaseNoLongerInUsedBlocks(TransactionContext tx)
        {
            var noLongerInUsedBlocks = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUsed))
                .ToImmutableArray();

            if (noLongerInUsedBlocks.Any())
            {
                _availableBlockTable.Query(tx)
                    .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUsed))
                    .Delete();
                _availableBlockTable.AppendRecords(noLongerInUsedBlocks
                    .Select(b => new AvailableBlockRecord(b.BlockId, BlockAvailability.Available)),
                    tx);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void IncrementBlockCount(TransactionContext tx)
        {
            var maxBlockId = _availableBlockTable.Query(tx)
                .OrderByDescending(a => a.BlockId)
                .Take(1)
                .Select(a => a.BlockId)
                .FirstOrDefault();
            var targetCapacity = maxBlockId + BLOCK_INCREMENT;
            var newBlockIds = Enumerable.Range(maxBlockId + 1, BLOCK_INCREMENT);
            var newAvailableBlocks = newBlockIds
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.Available));

            _dbFileManager.Value.EnsureBlockCapacity(targetCapacity);
            _availableBlockTable.AppendRecords(newAvailableBlocks, tx);
        }
    }
}
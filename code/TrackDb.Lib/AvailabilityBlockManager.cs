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
            var availableQuery = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available));

            while (blockIds.Count != blockIdCount)
            {
                var availableBlockIds = availableQuery
                    .Take(blockIdCount - blockIds.Count)
                    .AsEnumerable()
                    .Select(a => a.BlockId)
                    .ToArray();

                if (availableBlockIds.Length == 0)
                {
                    IncrementBlockCount(tx);
                }
                else
                {
                    availableQuery
                        .Where(pf => pf.In(a => a.BlockId, availableBlockIds))
                        .Delete();
                    _availableBlockTable.AppendRecords(
                        availableBlockIds
                        .Select(id => new AvailableBlockRecord(id, BlockAvailability.InUse)),
                        tx);
                    blockIds.AddRange(blockIds);
                }
            }
            ValidateValues(tx);

            return blockIds;
        }

        public void SetNoLongerInUse(IEnumerable<int> blockIds, TransactionContext tx)
        {
            var materializedBlockIds = blockIds.ToArray();
            var deletedCount = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.InUse))
                .Where(pf => pf.In(a => a.BlockId, materializedBlockIds))
                .Delete();

            if (deletedCount != materializedBlockIds.Length)
            {
                throw new InvalidOperationException(
                    $"InUse to available:  expected {materializedBlockIds.Length}, " +
                    $"found {deletedCount}");
            }
            _availableBlockTable.AppendRecords(
                materializedBlockIds
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.NoLongerInUse)),
                tx);
            ValidateValues(tx);
        }

        public IEnumerable<int> ResetNoLongerInUsed(TransactionContext tx)
        {
            var blockIds = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUse))
                .AsEnumerable()
                .Select(a => a.BlockId)
                .ToArray();

            _availableBlockTable.Query(tx)
                .Where(pf => pf.In(a => a.BlockId, blockIds))
                .Delete();
            _availableBlockTable.AppendRecords(
                blockIds
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.Available)),
                tx);
            ValidateValues(tx);

            return blockIds;
        }

        private void IncrementBlockCount(TransactionContext tx)
        {
            var maxBlockId = _availableBlockTable.Query(tx)
                .OrderByDescending(a => a.BlockId)
                .Take(1)
                .AsEnumerable()
                .Select(a => a.BlockId)
                .FirstOrDefault();
            var targetCapacity = maxBlockId + BLOCK_INCREMENT;
            var newAvailableBlocks = Enumerable.Range(maxBlockId + 1, BLOCK_INCREMENT)
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.Available));

            _dbFileManager.Value.EnsureBlockCapacity(targetCapacity);
            _availableBlockTable.AppendRecords(newAvailableBlocks, tx);
        }

        [Conditional("DEBUG")]
        private void ValidateValues(TransactionContext tx)
        {   //  Test for duplicates
            var duplicatedBlockIds = _availableBlockTable.Query(tx)
                .AsEnumerable()
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
            var minBlockId = _availableBlockTable.Query(tx)
                .OrderBy(a => a.BlockId)
                .Take(1)
                .AsEnumerable()
                .Select(a => a.BlockId)
                .First();
            var maxBlockId = _availableBlockTable.Query(tx)
                .OrderByDescending(a => a.BlockId)
                .Take(1)
                .AsEnumerable()
                .Select(a => a.BlockId)
                .First();
            var count = _availableBlockTable.Query(tx)
                .Count();

            if (minBlockId!=1)
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
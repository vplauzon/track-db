using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace TrackDb.Lib.DataLifeCycle
{
    /// <summary>
    /// Hard delete records by compacting blocks (and merging).
    /// Performs the following:
    /// 
    /// <list type="bullet">
    /// <item>
    /// Remove tombstoned blocks so the total remains under
    /// <see cref="TombstonePolicy.MaxTombstonedBlocks"/>.
    /// </item>
    /// <item>
    /// Remove blocks that are fully tombstoned.  This is done at the cadence of
    /// <see cref="TombstonePolicy.FullBlockPeriod"/>.
    /// </item>
    /// <item>
    /// Compact blocks that are partially tombstoned.  This is done at the cadence of
    /// <see cref="TombstonePolicy.PartialBlockPeriod"/> for blocks having at least
    /// a ratio of <see cref="TombstonePolicy.PartialBlockRatio"/>.
    /// </item>
    /// <item>
    /// Compact blocks that have been tombstoned and untouched for at least
    /// <see cref="TombstonePolicy.TombstoneRetentionPeriod"/>.
    /// </item>
    /// </list>
    /// </summary>
    internal class HardDeleteAgent : DataLifeCycleAgentBase
    {
        private DateTime _nextFull;
        private DateTime _nextPartial;
        private DateTime _nextRetention;

        public HardDeleteAgent(Database database)
            : base(database)
        {
            UpdateNextFull();
            UpdateNextPartial();
            UpdateNextRetention();
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var tombstoneMovementAgent = new TombstoneMovementLogic(Database);

                tombstoneMovementAgent.MoveTombstones(tx);
                CompactBlocks(forcedDataManagementActivity, tx);

                tx.Complete();
            }
        }

        private void CompactBlocks(
            DataManagementActivity forcedDataManagementActivity,
            TransactionContext tx)
        {
            var blockTombstonesIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex
                ?? (IDictionary<int, BlockTombstones>)
                tx.TransactionState.InMemoryDatabase.BlockTombstonesIndex;
            var forcedBlockIds = GetForcedBlockIds(
                forcedDataManagementActivity,
                blockTombstonesIndex,
                tx);
            var overBlockIds = GetOverBlockIds(blockTombstonesIndex, tx);
            var fullBlockIds = GetFullBlockIds(blockTombstonesIndex, tx);
            var partialBlockIds = GetPartialBlockIds(blockTombstonesIndex, tx);
            var retentionBlockIds = GetRetentionBlockIds(blockTombstonesIndex, tx);
            var unionedIds = forcedBlockIds
                .Concat(overBlockIds)
                .Concat(fullBlockIds)
                .Concat(partialBlockIds)
                .Concat(retentionBlockIds)
                .Distinct();
            var blockIdsGroupedByTableName = unionedIds
                .Select(id => blockTombstonesIndex[id])
                .GroupBy(t => t.TableName, t => t.BlockId)
                .ToArray();

            if (blockIdsGroupedByTableName.Length > 0)
            {
                if (tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex == null)
                {
                    blockTombstonesIndex = blockTombstonesIndex.ToDictionary();
                    tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex =
                        blockTombstonesIndex.ToDictionary();
                }

                foreach (var group in blockIdsGroupedByTableName)
                {
                    CompactTable(group.Key, group, blockTombstonesIndex, tx);
                }
            }
        }

        private void CompactTable(
            string tableName,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }

        #region Block IDs
        private int[] GetForcedBlockIds(
            DataManagementActivity forcedDataManagementActivity,
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            return forcedDataManagementActivity.HasFlag(DataManagementActivity.HardDeleteAll)
                ? blockTombstonesIndex.Keys.ToArray()
                : Array.Empty<int>();
        }

        private int[] GetOverBlockIds(
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            var trimCount = blockTombstonesIndex.Count
                - Database.DatabasePolicy.TombstonePolicy.MaxTombstonedBlocks;

            if (trimCount > 0)
            {
                var trimmedBlockTombstones = blockTombstonesIndex.Values
                    .OrderBy(t => t.ItemCount - t.DeletedCount)
                    .Take(trimCount);
                var trimmedBlockIds = trimmedBlockTombstones
                    .Select(t => t.BlockId)
                    .ToArray();

                return trimmedBlockIds;
            }
            else
            {
                return Array.Empty<int>();
            }
        }

        private int[] GetFullBlockIds(
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            if (_nextFull >= DateTime.Now)
            {
                var allDeletedBlockTombstones = blockTombstonesIndex.Values
                    .Where(t => t.IsAllDeleted);
                var allDeletedBlockIds = allDeletedBlockTombstones
                    .Select(t => t.BlockId)
                    .ToArray();

                UpdateNextFull();

                return allDeletedBlockIds;
            }
            else
            {
                return Array.Empty<int>();
            }
        }

        private int[] GetPartialBlockIds(
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            if (_nextPartial >= DateTime.Now)
            {
                var partialBlockRatio =
                    Database.DatabasePolicy.TombstonePolicy.PartialBlockRatio;
                var partialBlockTombstones = blockTombstonesIndex.Values
                    .Where(t => ((long)100) * partialBlockRatio * t.ItemCount <= t.DeletedCount);
                var allPartialBlockIds = partialBlockTombstones
                    .Select(t => t.BlockId)
                    .ToArray();

                UpdateNextPartial();

                return allPartialBlockIds;
            }
            else
            {
                return Array.Empty<int>();
            }
        }

        private int[] GetRetentionBlockIds(
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            if (_nextRetention >= DateTime.Now)
            {
                var tombstoneRetentionPeriod =
                    Database.DatabasePolicy.TombstonePolicy.TombstoneRetentionPeriod;
                var retentionBlockTombstones = blockTombstonesIndex.Values
                    .Where(t => t.SinceLastUpdated >= tombstoneRetentionPeriod);
                var retentionBlockIds = retentionBlockTombstones
                    .Select(t => t.BlockId)
                    .ToArray();

                UpdateNextRetention();

                return retentionBlockIds;
            }
            else
            {
                return Array.Empty<int>();
            }
        }
        #endregion

        #region Update Next runs
        private void UpdateNextFull()
        {
            _nextFull = DateTime.Now
                + Database.DatabasePolicy.TombstonePolicy.FullBlockPeriod;
        }

        private void UpdateNextPartial()
        {
            _nextPartial = DateTime.Now
                + Database.DatabasePolicy.TombstonePolicy.PartialBlockPeriod;
        }

        private void UpdateNextRetention()
        {
            _nextRetention = DateTime.Now
                + Database.DatabasePolicy.TombstonePolicy.TombstoneRetentionPeriod;
        }
        #endregion
    }
}
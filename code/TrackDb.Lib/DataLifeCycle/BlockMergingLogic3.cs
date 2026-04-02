using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic3 : LogicBase
    {
        private static readonly IDictionary<int, TombstoneBlock> _emptyTombstoneBlock =
            new Dictionary<int, TombstoneBlock>();

        private readonly MetaBlockMergingLogic _metaBlockMergingLogic;

        public BlockMergingLogic3(Database database)
            : base(database)
        {
            _metaBlockMergingLogic = new MetaBlockMergingLogic(Database);
        }

        public void CompactMerge(
            IDictionary<string, IEnumerable<int>> blockIdsToCompactByTableName,
            IDictionary<string, IEnumerable<TombstoneBlock>> allTombstoneBlocksMap,
            TransactionContext tx)
        {
            foreach (var pair in blockIdsToCompactByTableName)
            {
                var tableName = pair.Key;
                var blockIdsToCompact = pair.Value;
                var allTableTombstoneBlocksIndex = allTombstoneBlocksMap[tableName]
                    .ToDictionary(t => t.BlockId);

                CompactMergeTable(tableName, blockIdsToCompact, allTableTombstoneBlocksIndex, tx);
            }
        }

        private void CompactMergeTable(
            string tableName,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var tombstoneBlocksGroups = blockIdsToCompact
                .Select(id => allTombstoneBlockIndex[id])
                .GroupBy(t => t.BlockTraces.Count)
                .Select(g => g.ToArray());
            var cumulatedDeletedBlockIds = new List<int>();
            var blockReplacementMap = new Dictionary<int, IEnumerable<MetadataBlock>>();
#if DEBUG
            var tombstoneCountBefore = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Count();
            var tableNoDeleteCountBefore = Database.GetAnyTable(tableName).Query(tx)
                .WithIgnoreDeleted()
                .Count();
            var tableCountBefore = Database.GetAnyTable(tableName).Query(tx).Count();
#endif

            foreach (var tombstoneBlocks in tombstoneBlocksGroups)
            {   //  Each of those plans are independant as the root is at different level
                var traceLength = tombstoneBlocks[0].BlockTraces.Count;

                for (var i = traceLength - 1; i >= 0; --i)
                {
                    var tombstoneBlocksByMetaBlockId = tombstoneBlocks
                        .GroupBy(t => t.BlockTraces[i].BlockId)
                        .Distinct();

                    foreach (var metaBlockGroup in tombstoneBlocksByMetaBlockId)
                    {
                        var metaBlockId = metaBlockGroup.Key <= 0 ? (int?)null : metaBlockGroup.Key;
                        var result = _metaBlockMergingLogic.CompactMerge(
                            metaBlockId,
                            (MetadataTableSchema)metaBlockGroup.First().BlockTraces[i].Schema,
                            i == traceLength - 1
                            ? metaBlockGroup.Select(o => o.BlockId)
                            : Array.Empty<int>(),
                            i == traceLength - 1 ? allTombstoneBlockIndex : _emptyTombstoneBlock,
                            blockReplacementMap,
                            tx);

                        cumulatedDeletedBlockIds.AddRange(result.DeletedBlockIds);
                        if (metaBlockId != null)
                        {
                            blockReplacementMap.Add(metaBlockId.Value, result.MetaBlocks);
                            cumulatedDeletedBlockIds.Add(metaBlockId.Value);
                        }
                    }
                }
                blockReplacementMap.Clear();
            }
            CleanDeletedBlocksAndRecords(
                tableName, cumulatedDeletedBlockIds, allTombstoneBlockIndex, tx);
#if DEBUG
            var table = Database.GetAnyTable(tableName);
            var tombstoneCountAfter = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Count();
            var tableNoDeleteCountAfter = Database.GetAnyTable(tableName).Query(tx)
                .WithIgnoreDeleted()
                .Count();
            var tableCountAfter = Database.GetAnyTable(tableName).Query(tx).Count();

            if (tableCountBefore != tableCountAfter)
            {
                throw new InvalidOperationException("Corrupted table count with delete");
            }
            if (tombstoneCountBefore <= tombstoneCountAfter)
            {
                throw new InvalidOperationException("Tombstone count increased or stay the same");
            }
            if (tableNoDeleteCountBefore <= tableNoDeleteCountAfter)
            {
                throw new InvalidOperationException("Corrupted table count without delete");
            }
#endif
        }

        private void CleanDeletedBlocksAndRecords(
            string tableName,
            IReadOnlyList<int> cumulatedDeletedBlockIds,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var deletedRecordIds = cumulatedDeletedBlockIds
                .SelectMany(id => allTombstoneBlockIndex.TryGetValue(id, out var tb)
                ? tb.RecordIds
                : Array.Empty<long>());

            Database.AvailabilityBlockManager.SetNoLongerInUseBlockIds(cumulatedDeletedBlockIds, tx);
            Database.DeleteTombstoneRecords(tableName, deletedRecordIds, tx);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic3 : LogicBase
    {
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

                CompactMergeTable(blockIdsToCompact, allTombstoneBlocksMap[tableName], tx);
            }
        }

        private void CompactMergeTable(
            IEnumerable<int> blockIdsToCompact,
            IEnumerable<TombstoneBlock> allTombstoneBlocks,
            TransactionContext tx)
        {
            var allTombstoneBlockIndex = allTombstoneBlocks
                .ToDictionary(t => t.BlockId);
            var planGroupedByRoot = blockIdsToCompact
                .Select(id => allTombstoneBlockIndex[id])
                .GroupBy(t => new
                {
                    RootBlockId = t.BlockTraces.First().BlockId,
                    RootTableName = t.Schema.TableName
                });

            foreach (var rootPlan in planGroupedByRoot)
            {
                var planGroupByMetaBlockId = rootPlan
                    .GroupBy(t => t.BlockTraces.Last().BlockId);

                EnsureTraceLength(rootPlan);
                foreach (var metaBlockGroup in planGroupByMetaBlockId)
                {
                    _metaBlockMergingLogic.CompactMerge(
                        metaBlockGroup.Key,
                        metaBlockGroup.First().Schema,
                        metaBlockGroup.Select(o => o.BlockId),
                        allTombstoneBlockIndex,
                        tx);
                }
            }
        }

        [Conditional("DEBUG")]
        private void EnsureTraceLength(IEnumerable<TombstoneBlock> plan)
        {
            var traceLength = plan.First().BlockTraces.Count;

            foreach(var block in plan.Skip(1))
            {
                if (block.BlockTraces.Count != traceLength)
                {
                    throw new InvalidOperationException($"Inconsistent trace lengths in plan");
                }
            }
        }
    }
}
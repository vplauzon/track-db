using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic3 : LogicBase
    {
        public BlockMergingLogic3(Database database)
            : base(database)
        {
        }

        public void CompactMerge(
            IDictionary<string, IEnumerable<TombstoneBlock>> plan,
            IDictionary<string, IEnumerable<TombstoneBlock>> allTombstoneBlocksMap,
            TransactionContext tx)
        {
            foreach (var tableName in plan.Keys)
            {
                CompactMergeTable(plan[tableName], allTombstoneBlocksMap[tableName], tx);
            }
        }

        private void CompactMergeTable(
            IEnumerable<TombstoneBlock> plan,
            IEnumerable<TombstoneBlock> allTombstoneBlocks,
            TransactionContext tx)
        {
            var planGroupedByRoot = plan
                .GroupBy(t => new
                {
                    RootBlockId = t.BlockTraces.First().BlockId,
                    RootTableName = t.Schema.TableName
                });
            var allTombstoneBlockIndex = allTombstoneBlocks
                .ToDictionary(t => t.BlockId);

            foreach (var rootPlan in planGroupedByRoot)
            {
                var planGroupByMetaBlockId = rootPlan
                    .GroupBy(t => t.BlockTraces.Last().BlockId);

                EnsureTraceLength(rootPlan);
                foreach (var metaBlockGroup in planGroupByMetaBlockId)
                {
                    CompactMergeMetaBlock(
                        metaBlockGroup.Key,
                        metaBlockGroup.First().BlockTraces.Last().Schema,
                        metaBlockGroup,
                        allTombstoneBlockIndex,
                        tx);
                }
            }
        }

        private void CompactMergeMetaBlock(
            int metaBlockId,
            TableSchema schema,
            IEnumerable<TombstoneBlock> tombstoneBlocks,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            throw new NotImplementedException();
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
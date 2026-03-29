using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
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
                CompactMergeTable(tableName, plan[tableName], allTombstoneBlocksMap[tableName], tx);
            }
        }

        private void CompactMergeTable(
            string tableName,
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

            throw new NotImplementedException();
        }
    }
}
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
            IDictionary<string, IEnumerable<TombstoneBlock>> tombstoneBlocksMap,
            TransactionContext tx)
        {
            foreach (var tableName in plan.Keys)
            {
                CompactMergeTable(tableName, plan[tableName], tombstoneBlocksMap[tableName], tx);
            }
        }

        private void CompactMergeTable(string tableName, IEnumerable<TombstoneBlock> enumerable1, IEnumerable<TombstoneBlock> enumerable2, TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
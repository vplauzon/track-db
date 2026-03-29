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
            throw new NotImplementedException();
        }
    }
}
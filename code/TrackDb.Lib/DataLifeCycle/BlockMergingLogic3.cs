using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic3 : LogicBase
    {
        public BlockMergingLogic3(Database database)
            : base(database)
        {
        }

        public void CompactMerge(
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
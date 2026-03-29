using System;
using System.Collections.Generic;
using System.Linq;
using TrackDb.Lib.DataLifeCycle.Persistance;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaBlockMergingLogic : LogicBase
    {
        public MetaBlockMergingLogic(Database database)
            : base(database)
        {
        }

        public void CompactMerge(
            int metaBlockId,
            TableSchema schema,
            IEnumerable<TombstoneBlock> tombstoneBlocks,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var metaBlockManager = new MetaBlockManager(Database, tx);
            var blocks = metaBlockManager.LoadBlocks(
                schema.TableName,
                metaBlockId <= 0 ? null : metaBlockId);

            throw new NotImplementedException();
        }
    }
}
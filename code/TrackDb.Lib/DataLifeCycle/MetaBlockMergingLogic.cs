using System;
using System.Collections.Generic;
using System.Linq;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaBlockMergingLogic : LogicBase
    {
        public MetaBlockMergingLogic(Database database)
            : base(database)
        {
        }

        internal void CompactMerge(
            int metaBlockId,
            TableSchema metaSchema,
            IEnumerable<TombstoneBlock> tombstoneBlocks,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var metaBlock = metaBlockId > 0
                ? Database.GetOrLoadBlock(metaBlockId, metaSchema)
                : GetInMemoryBlock(metaSchema, tx);
            var q = ((ReadOnlyBlock)metaBlock).DebugView;

            throw new NotImplementedException();
        }

        private static IBlock GetInMemoryBlock(TableSchema schema, TransactionContext tx)
        {
            return tx
                .TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[schema.TableName]
                .CommittedDataBlock!;
        }
    }
}
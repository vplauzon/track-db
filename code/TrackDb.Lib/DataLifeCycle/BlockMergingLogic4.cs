using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic4 : LogicBase
    {
        #region Inner Types
        private record TombstoneBlock(
            IReadOnlyList<BlockTrace> BlockTraces,
            //  Schema of the block's table
            TableSchema Schema,
            int BlockId);
        #endregion

        public BlockMergingLogic4(Database database)
            : base(database)
        {
        }

        public void CompactMerge(
            string tableName,
            IEnumerable<int> blockIdsToCompact,
            TransactionContext tx)
        {
            var blockIdsToCompactSet = blockIdsToCompact.ToFrozenSet();
            var q = Q(tableName, blockIdsToCompactSet, tx);
        }

        private object Q(
            string tableName,
            ISet<int> blockIdsToCompact,
            TransactionContext tx)
        {
            var metaTable = Database.GetMetaDataTable(tableName);
            var metaSchema = (MetadataTableSchema)metaTable.Schema;
            var predicate = new InPredicate<int>(
                metaSchema.BlockIdColumnIndex,
                blockIdsToCompact,
                true);
            var results = metaTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection(metaSchema.BlockIdColumnIndex)
                .ExecuteQueryWithBlockTrace()
                .Select(r => new
                {
                    BlockId = (int)r.Result.Span[0]!,
                    r.BlockTraces
                });

            throw new NotImplementedException();
        }
    }
}
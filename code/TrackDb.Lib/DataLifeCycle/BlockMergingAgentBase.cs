using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record Q(long? parentBlockId, long DeletedRecordId);
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        /// <summary>
        /// Merges the <paramref name="blockId"/> with all other blocks within its metablock.
        /// </summary>
        /// <param name="metadataTableName"></param>
        /// <param name="blockId"></param>
        /// <param name="tx"></param>
        /// <returns><c>true</c> iif at least one block was merged or changed.</returns>
        protected bool MergeBlock(string metadataTableName, int blockId, TransactionContext tx)
        {
            var parentBlockId = FindParentBlock(metadataTableName, blockId, tx);

            throw new NotImplementedException();
        }

        protected bool MergeSubBlocks(string metadataTableName, int? metaBlockId, TransactionContext tx)
        {
            throw new NotImplementedException();
        }

        private int? FindParentBlock(
            string metadataTableName,
            int blockId,
            TransactionContext tx)
        {
            var metadataTable = Database.GetAnyTable(metadataTableName);
            var predicate = new BinaryOperatorPredicate(
                ((MetadataTableSchema)metadataTable.Schema).BlockIdColumnIndex,
                blockId,
                BinaryOperator.Equal);
            var parentBlockId = metadataTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection([metadataTable.Schema.ParentBlockIdColumnIndex])
                .Select(r => (int)r.Span[0]!)
                .FirstOrDefault();

            return parentBlockId > 0 ? parentBlockId : null;
        }

    }
}
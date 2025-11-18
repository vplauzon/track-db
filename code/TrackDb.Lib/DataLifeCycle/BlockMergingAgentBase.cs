using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record BlockInfo(
            int BlockId,
            long MinRecordId);
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        /// <summary>
        /// Compact (remove deleted records) blocks from a data table (generation 1) and merge them
        /// together, updating the meta blocks "in place".
        /// </summary>
        /// <param name="dataTableName"></param>
        /// <param name="deletedRecordId">Starting record ID in the table.</param>
        /// <param name="tx"></param>
        /// <returns></returns>
        protected bool MergeDataBlocks(string dataTableName, long deletedRecordId, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var metadataTableName = tableMap[dataTableName].MetaDataTableName;

            if (tableMap[dataTableName].IsMetaDataTable)
            {
                throw new ArgumentException($"Table is metadata", nameof(dataTableName));
            }
            if (metadataTableName == null)
            {
                throw new ArgumentException($"Table has no corresponding metadata table", nameof(dataTableName));
            }
            var deletedRecordBlockId = FindDataBlock(dataTableName, deletedRecordId, tx);

            if (deletedRecordBlockId == null)
            {   //  Record doesn't exist anymore:  likely a racing condition between 2 transactions
                //  (should be rare)
                Database.TombstoneTable.Query(tx)
                    .Where(pf => pf.Equal(t => t.TableName, dataTableName))
                    .Where(pf => pf.Equal(t => t.DeletedRecordId, deletedRecordId))
                    .Delete();

                return true;
            }
            else
            {
                var parentBlockId = FindParentBlock(metadataTableName, deletedRecordBlockId.Value, tx);

                return MergeSubBlocks(metadataTableName, parentBlockId, tx);
            }
        }

        /// <summary>
        /// Merges all children blocks of <paramref name="metaBlockId"/>.
        /// </summary>
        /// <param name="metadataTableName">
        /// Metadata table where the blocks (not <paramref name="metaBlockId"/>) live
        /// </param>
        /// <param name="metaBlockId"></param>
        /// <param name="tx"></param>
        /// <returns></returns>
        protected bool MergeSubBlocks(string metadataTableName, int? metaBlockId, TransactionContext tx)
        {
            //var blocks = LoadBlocks(metadataTableName, metaBlockId, tx);
            throw new NotImplementedException();
        }

        #region Find blocks
        private int? FindDataBlock(string tableName, long deletedRecordId, TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
            var predicate = new BinaryOperatorPredicate(
                table.Schema.RecordIdColumnIndex,
                deletedRecordId,
                BinaryOperator.Equal);
            var blockId = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection([table.Schema.ParentBlockIdColumnIndex])
                .Select(r => (int?)r.Span[0])
                .FirstOrDefault();

            return blockId;
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
        #endregion

        private IImmutableList<BlockInfo> LoadBlocks(
            string metadataTableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            var metaBlock = LoadMetaBlock(metadataTableName, metaBlockId, tx);

            throw new NotImplementedException();
        }

        private IBlock LoadMetaBlock(
            string metadataTableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

            if (metaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {   //  We merge blocks in memory
                tx.LoadCommittedBlocksInTransaction(metadataTableName);

                return tx.TransactionState.InMemoryDatabase
                    .TransactionTableLogsMap[metadataTableName]
                    .InMemoryBlocks
                    .First();
            }
        }
    }
}
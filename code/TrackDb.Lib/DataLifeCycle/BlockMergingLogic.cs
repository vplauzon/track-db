using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic : LogicBase
    {
        #region Inner Types
        private interface IBlockFacade
        {
            long ComputeRecordIdMax();
        }

        private record MetaDataBlockFacade(MetaDataBlock MetaDataBlock) : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax() => MetaDataBlock.RecordIdMin;
        }

        private record BlockBuilderFacade(BlockBuilder BlockBuilder) : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax()
            {
                var block = ((IBlock)BlockBuilder);
                var recordIdMax = block.Project(
                    new object?[1],
                    [block.TableSchema.RecordIdColumnIndex],
                    Enumerable.Range(0, block.RecordCount),
                    0)
                    .Select(r => (long)r.Span[0]!)
                    .Max();

                return recordIdMax;
            }
        }
        #endregion

        public BlockMergingLogic(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Compact block <paramref name="blockId"/> in table <paramref name="dataTableName"/>.
        /// This block is going to be compacted and then merged with adjacent blocks.
        /// If any block in <paramref name="otherBlockIdsToCompact"/> is encountered during
        /// those merge operations, they are going to be compacted.  If not, they won't.
        /// </summary>
        /// <param name="dataTableName"></param>
        /// <param name="blockId"></param>
        /// <param name="otherBlockIdsToCompact"></param>
        /// <param name="tx"></param>
        /// <returns><c>false</c> iif <paramref name="blockId"/> doesn't exist</returns>
        public bool CompactBlock(
            string dataTableName,
            int blockId,
            IEnumerable<int> otherBlockIdsToCompact,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tableProperties = tableMap[dataTableName];
            var schema = tableProperties.Table.Schema;
            var metadataTableName = tableProperties.MetaDataTableName;

            if (tableProperties.IsMetaDataTable)
            {
                throw new ArgumentException($"Table is metadata", nameof(dataTableName));
            }
            if (metadataTableName == null)
            {
                throw new ArgumentException(
                    $"Table has no corresponding metadata table",
                    nameof(dataTableName));
            }

            var metadataTableProperties = tableMap[metadataTableName];
            var metadataTable = metadataTableProperties.Table;
            var metadataSchema = (MetadataTableSchema)metadataTable.Schema;
            var predicate = new BinaryOperatorPredicate(
                metadataSchema.BlockIdColumnIndex,
                blockId,
                BinaryOperator.Equal);
            var metaRecord = metadataTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection([metadataSchema.ParentBlockIdColumnIndex])
                .FirstOrDefault();

            if (metaRecord.Length == 1)
            {
                var metaBlockId = (int)metaRecord.Span[0]!;

                MergeBlocks(
                    metadataTableName,
                    metaBlockId != 0 ? metaBlockId : null,
                    otherBlockIdsToCompact.Prepend(blockId),
                    tx);

                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Merge meta block <paramref name="metaBlockId"/> in meta table
        /// <paramref name="metaTableName"/> and compact blocks in
        /// <paramref name="blockIdsToCompact"/> if they are encountered.
        /// </summary>
        /// <param name="metaTableName"></param>
        /// <param name="metaBlockId"></param>
        /// <param name="blockIdsToCompact"></param>
        /// <param name="tx"></param>
        public void MergeBlocks(
            string metaTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            TransactionContext tx)
        {
            MergeBlocksWithReplacements(
                metaTableName,
                metaBlockId,
                blockIdsToCompact,
                Array.Empty<int>(),
                Array.Empty<BlockBuilder>(),
                tx);
        }

        private void MergeBlocksWithReplacements(
            string metaTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            var blockStack = LoadBlockFacades(
                metaTableName,
                metaBlockId,
                blockIdsToRemove,
                blocksToAdd,
                tx);

            throw new NotImplementedException();
        }

        #region Load block infos
        private Stack<IBlockFacade> LoadBlockFacades(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            if (metaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                var metadataTable = Database.GetDatabaseStateSnapshot()
                    .TableMap[metadataTableName]
                    .Table;
                var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
                var blockIdsToRemoveSet = blockIdsToRemove.ToImmutableHashSet();
                var metaDataBlockFacades = metadataTable.Query(tx)
                    .WithInMemoryOnly()
                    .Select(r => new MetaDataBlock(r, metadataTableSchema))
                    .Where(b => !blockIdsToRemoveSet.Contains(b.BlockId))
                    .Select(b => new MetaDataBlockFacade(b));
                var blockBuilderFacades = blocksToAdd
                    .Select(b => new BlockBuilderFacade(b));
                var allFacades = metaDataBlockFacades
                    .Cast<IBlockFacade>()
                    .Concat(blockBuilderFacades)
                    .Select(f => new
                    {
                        Facade = f,
                        RecordIdMax = f.ComputeRecordIdMax()
                    })
                    //  Order by record ID to keep monotonity of record IDs
                    .OrderBy(f => f.RecordIdMax)
                    .Select(f => f.Facade);

                return new Stack<IBlockFacade>(allFacades);
            }
        }
        #endregion
    }
}
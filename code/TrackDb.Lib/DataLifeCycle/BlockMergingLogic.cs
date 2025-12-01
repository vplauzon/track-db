using System;
using System.Collections;
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

            int ComputeSize();

            IBlockFacade? CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx);

            BlockBuilderFacade? ForceCompact(TransactionContext tx);

            MetaDataBlockFacade Persist(TransactionContext tx);
        }

        private record MetaDataBlockFacade(Database Database, MetaDataBlock MetaDataBlock)
            : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax() => MetaDataBlock.RecordIdMin;

            int IBlockFacade.ComputeSize() => MetaDataBlock.Size;

            IBlockFacade? IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                if (blockIdsToCompact.Contains(MetaDataBlock.BlockId))
                {
                    return ((IBlockFacade)this).ForceCompact(tx);
                }
                else
                {
                    return this;
                }
            }

            BlockBuilderFacade? IBlockFacade.ForceCompact(TransactionContext tx)
            {
                var parentSchema = MetaDataBlock.Schema.ParentSchema;
                var block = Database.GetOrLoadBlock(MetaDataBlock.BlockId, parentSchema);
                //  First let's find the record IDs in the block
                var blockRecordIds = block.Project(
                    new object?[1],
                    [parentSchema.RecordIdColumnIndex],
                    Enumerable.Range(0, block.RecordCount),
                    0)
                    .Select(r => (long)r.Span[0]!)
                    .ToImmutableArray();
                //  Then, let's find which ones are deleted
                var deletedRecordIds = Database.TombstoneTable.Query(tx)
                    .Where(pf => pf.Equal(t => t.TableName, parentSchema.TableName))
                    .Where(pf => pf.In(t => t.DeletedRecordId, blockRecordIds))
                    .Select(t => t.DeletedRecordId)
                    .ToImmutableArray();
                var blockBuilder = new BlockBuilder(parentSchema);

                //  Hard delete those records
                blockBuilder.AppendBlock(block);
                blockBuilder.DeleteRecordsByRecordId(deletedRecordIds);
                Database.DeleteTombstoneRecords(parentSchema.TableName, deletedRecordIds, tx);

                return ((IBlock)blockBuilder).RecordCount > 0
                    ? new BlockBuilderFacade(Database, blockBuilder)
                    : null;
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                return this;
            }
        }

        private record BlockBuilderFacade(Database Database, BlockBuilder BlockBuilder)
            : IBlockFacade
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

            int IBlockFacade.ComputeSize()
            {
                var blockStats = BlockBuilder.Serialize(Array.Empty<byte>());

                return blockStats.Size;
            }

            IBlockFacade? IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                return this;
            }

            BlockBuilderFacade? IBlockFacade.ForceCompact(TransactionContext tx)
            {
                return this;
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
                var blockStats = BlockBuilder.Serialize(buffer);

                if (blockStats.Size > buffer.Length)
                {
                    throw new InvalidOperationException("Block bigger than planned");
                }

                var blockId = Database.PersistBlock(buffer.AsSpan().Slice(0, blockStats.Size), tx);
                var tableName = ((IBlock)BlockBuilder).TableSchema.TableName;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var metadataTable = tableMap[tableMap[tableName].MetaDataTableName!].Table;
                var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                var metaRecord = metaSchema.CreateMetadataRecord(blockId, blockStats);
                var metaBlock = new MetaDataBlock(metaRecord, metaSchema);

                return new MetaDataBlockFacade(Database, metaBlock);
            }

            public BlockBuilderFacade Merge(
                BlockBuilderFacade right,
                TransactionContext tx)
            {
                var newBlockBuilder =
                    new BlockBuilder(((IBlock)BlockBuilder).TableSchema);

                newBlockBuilder.AppendBlock(BlockBuilder);
                newBlockBuilder.AppendBlock(right.BlockBuilder);

                return new BlockBuilderFacade(Database, newBlockBuilder);
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

                return MergeBlocks(
                    metadataTableName,
                    metaBlockId != 0 ? metaBlockId : null,
                    otherBlockIdsToCompact.Prepend(blockId),
                    tx);
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
        /// <returns><c>true</c> iif some changes happened</returns>
        public bool MergeBlocks(
            string metaTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            TransactionContext tx)
        {
            return MergeBlocksWithReplacements(
                metaTableName,
                metaBlockId,
                blockIdsToCompact,
                Array.Empty<int>(),
                Array.Empty<BlockBuilder>(),
                tx);
        }

        private bool MergeBlocksWithReplacements(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            while (true)
            {
                var newBlockBuilder = MergeBlocksWithReplacementsOneLayer(
                    metadataTableName,
                    metaBlockId,
                    blockIdsToCompact,
                    blockIdsToRemove,
                    blocksToAdd,
                    tx);

                if (newBlockBuilder == null)
                {   //  Nothing changed
                    return false;
                }
                else
                {
                    if (metaBlockId == null)
                    {   //  The top of the hierarchy
                        var metadataTable = Database.GetAnyTable(metadataTableName);

                        //  Delete all in-memory meta records
                        metadataTable.Query(tx)
                            .WithInMemoryOnly()
                            .Delete();
                        tx.LoadCommittedBlocksInTransaction(metadataTable.Schema.TableName);
                        tx.TransactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[metadataTable.Schema.TableName]
                            .NewDataBlock
                            .AppendBlock(newBlockBuilder);

                        return true;
                    }
                    else
                    {   //  Climb the hierarchy
                        var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                        var metaMetaDataTableName = tableMap[metadataTableName].MetaDataTableName!;
                        var metaMetaDataTable = Database.GetAnyTable(metaMetaDataTableName);
                        var schema = (MetadataTableSchema)metaMetaDataTable.Schema;
                        var predicate = new BinaryOperatorPredicate(
                            schema.BlockIdColumnIndex,
                            metaBlockId,
                            BinaryOperator.Equal);
                        var result = metaMetaDataTable.Query(tx)
                            .WithPredicate(predicate)
                            .WithProjection([schema.ParentBlockIdColumnIndex])
                            .FirstOrDefault();

                        if (result.Length == 0)
                        {
                            throw new InvalidOperationException($"Can't query meta block");
                        }

                        var metaMetaBlockId = (int)result.Span[0]!;

                        return MergeBlocksWithReplacements(
                            metaMetaDataTableName,
                            metaMetaBlockId != 0 ? metaMetaBlockId : null,
                            Array.Empty<int>(),
                            [metaBlockId.Value],
                            [newBlockBuilder],
                            tx);
                    }
                }
            }
        }

        private BlockBuilder? MergeBlocksWithReplacementsOneLayer(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            (var blockStack, var originalBlockIds) = LoadBlockFacades(
                metadataTableName,
                metaBlockId,
                blockIdsToRemove,
                blocksToAdd,
                tx);
            var metadataTable = Database.GetAnyTable(metadataTableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var mergedMetaBlocks = MergeBlockStack(
                blockStack,
                blockIdsToCompact.ToImmutableHashSet(),
                metaSchema,
                tx);
            var newBlockIds = mergedMetaBlocks
                .Select(m => m.BlockId)
                .ToImmutableArray();
            var removedBlockIds = originalBlockIds.Except(newBlockIds);

            if (removedBlockIds.Any())
            {
                var newMetadataBlockBuilder = new BlockBuilder(metaSchema);

                Database.SetNoLongerInUsedBlockIds(removedBlockIds, tx);
                foreach (var metaBlock in mergedMetaBlocks)
                {
                    newMetadataBlockBuilder.AppendRecord(
                        DateTime.Now,
                        metadataTable.NewRecordId(),
                        metaBlock.MetadataRecord.Span);
                }

                return newMetadataBlockBuilder;
            }
            else
            {
                return null;
            }
        }

        #region Block loading
        private IImmutableList<MetaDataBlock> LoadMetaDataBlocks(
            string metadataTableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            if (metaBlockId != null)
            {
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var metaMetadataTableName = tableMap[metadataTableName].MetaDataTableName;
                var metaMetadataTable = Database.GetAnyTable(metaMetadataTableName!);
                var metadataTable = Database.GetAnyTable(metadataTableName!);
                var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
                var block = Database.GetOrLoadBlock(metaBlockId.Value, metadataTable.Schema);
                var results = block.Project(
                    new object?[metadataTable.Schema.Columns.Count],
                    Enumerable.Range(0, metadataTable.Schema.Columns.Count).ToImmutableArray(),
                    Enumerable.Range(0, block.RecordCount),
                    0);
                var metaDataBlocks = results
                    .Select(r => new MetaDataBlock(r.ToArray(), metadataTableSchema))
                    .ToImmutableArray();

                return metaDataBlocks;
            }
            else
            {
                var metadataTable = Database.GetAnyTable(metadataTableName);
                var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
                var metaDataBlocks = metadataTable.Query(tx)
                    .WithInMemoryOnly()
                    .Select(r => new MetaDataBlock(r.ToArray(), metadataTableSchema))
                    .ToImmutableArray();

                return metaDataBlocks;
            }
        }

        private (Stack<IBlockFacade> BlockFacades, IEnumerable<int> OriginalBlockIds) LoadBlockFacades(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            var metaDataBlocks = LoadMetaDataBlocks(metadataTableName, metaBlockId, tx);
            var originalBlockIds = metaDataBlocks
                .Select(m => m.BlockId)
                .ToImmutableArray();
            var blockIdsToRemoveSet = blockIdsToRemove.ToImmutableHashSet();
            var metaDataBlockFacades = metaDataBlocks
                .Where(b => !blockIdsToRemoveSet.Contains(b.BlockId))
                .Select(b => new MetaDataBlockFacade(Database, b));
            var blockBuilderFacades = blocksToAdd
                .Select(b => new BlockBuilderFacade(Database, b));
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

            return (new Stack<IBlockFacade>(allFacades), originalBlockIds);
        }
        #endregion

        #region Merge algorithm
        private IEnumerable<MetaDataBlock> MergeBlockStack(
            Stack<IBlockFacade> blockStack,
            IImmutableSet<int> blockIdsToCompact,
            MetadataTableSchema metaSchema,
            TransactionContext tx)
        {
            var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
            var processedBlocks = new List<MetaDataBlock>(blockStack.Count);

            while (blockStack.Any())
            {
                var leftBlock = blockStack.Pop().CompactIf(blockIdsToCompact, tx);

                if (leftBlock != null)
                {
                    if (blockStack.Any())
                    {
                        var rightBlock = blockStack.Pop().CompactIf(blockIdsToCompact, tx);

                        if (rightBlock != null)
                        {
                            if (leftBlock.ComputeSize() + rightBlock.ComputeSize() <= maxBlockSize)
                            {
                                var leftCompacted = leftBlock.ForceCompact(tx);
                                var rightCompacted = rightBlock.ForceCompact(tx);

                                if (leftCompacted == null && rightCompacted != null)
                                {
                                    blockStack.Push(rightCompacted);
                                }
                                else if (leftCompacted != null && rightCompacted == null)
                                {
                                    blockStack.Push(leftCompacted);
                                }
                                else if (leftCompacted != null && rightCompacted != null)
                                {
                                    var resultingBlock = leftCompacted.Merge(rightCompacted, tx);

                                    if (((IBlockFacade)resultingBlock).ComputeSize() <= maxBlockSize)
                                    {
                                        blockStack.Push(resultingBlock);
                                    }
                                    else
                                    {   //  Blocks can't be merged
                                        processedBlocks.Add(leftBlock.Persist(tx).MetaDataBlock);
                                        blockStack.Push(rightBlock);
                                    }
                                }
                                else
                                {   //  Nothing:  both blocks disappeared
                                }
                            }
                            else
                            {   //  Blocks can't be merged
                                processedBlocks.Add(leftBlock.Persist(tx).MetaDataBlock);
                                blockStack.Push(rightBlock);
                            }
                        }
                        else
                        {   //  Right block disappeared
                            blockStack.Push(leftBlock);
                        }
                    }
                    else
                    {   //  We're done
                        processedBlocks.Add(leftBlock.Persist(tx).MetaDataBlock);
                    }
                }
            }

            return processedBlocks;
        }
        #endregion
    }
}
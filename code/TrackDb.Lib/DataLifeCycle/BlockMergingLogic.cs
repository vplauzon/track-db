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

            int ItemCount { get; }

            /// <summary>
            /// Compact block if the block is within <paramref name="blockIdsToCompact"/>
            /// or if <paramref name="blockIdsToCompact"/> is empty.
            /// </summary>
            /// <param name="blockIdsToCompact"></param>
            /// <param name="tx"></param>
            /// <returns>
            /// <c>null</c> iif the block didn't get compacted.
            /// Returns the new block and the list of record IDs that were hard deleted.
            /// </returns>
            (BlockBuilderFacade NewBlock, IEnumerable<long> HardDeletedRecordIds)? Compact(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx);

            MetaDataBlockFacade Persist(TransactionContext tx);
        }

        private record MetaDataBlockFacade(Database Database, MetaDataBlock MetaDataBlock)
            : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax() => MetaDataBlock.RecordIdMin;

            int IBlockFacade.ComputeSize() => MetaDataBlock.Size;

            int IBlockFacade.ItemCount => MetaDataBlock.ItemCount;

            (BlockBuilderFacade NewBlock, IEnumerable<long> HardDeletedRecordIds)? IBlockFacade.Compact(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                if (blockIdsToCompact.Contains(MetaDataBlock.BlockId))
                {
                    var parentSchema = MetaDataBlock.Schema.ParentSchema;
                    var block = Database.GetOrLoadBlock(MetaDataBlock.BlockId, parentSchema);
                    //  First let's find the record IDs in the block
                    var allRecordIds = block.Project(
                        new object?[1],
                        [parentSchema.RecordIdColumnIndex],
                        Enumerable.Range(0, block.RecordCount),
                        0)
                        .Select(r => (long)r.Span[0]!)
                        .ToImmutableArray();
                    //  Then, let's find which ones are deleted
                    var deletedRecordIds = Database.TombstoneTable.Query(tx)
                        .Where(pf => pf.Equal(t => t.TableName, parentSchema.TableName))
                        .Where(pf => pf.In(t => t.DeletedRecordId, allRecordIds))
                        .Select(t => t.DeletedRecordId)
                        .ToImmutableArray();
                    var blockBuilder = new BlockBuilder(parentSchema);

                    //  Hard delete those records
                    blockBuilder.AppendBlock(block);
                    blockBuilder.DeleteRecordsByRecordId(deletedRecordIds);

                    return (new BlockBuilderFacade(Database, blockBuilder), deletedRecordIds);
                }
                else
                {
                    return null;
                }
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

            int IBlockFacade.ItemCount => ((IBlock)BlockBuilder).RecordCount;

            (BlockBuilderFacade NewBlock, IEnumerable<long> HardDeletedRecordIds)? IBlockFacade.Compact(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                return null;
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
                throw new ArgumentException(
                    $"Table '{tableProperties.Table.Schema.TableName}' is metadata",
                    nameof(dataTableName));
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
                    metaBlockId > 0 ? metaBlockId : null,
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
            var cumulatedHardDeletedRecordIds = new Dictionary<string, IEnumerable<long>>();
            var cumulatedReleasedBlockIds = new List<int>();

            while (true)
            {
                var mergeResults = MergeBlocksWithReplacementsOneLayer(
                    metadataTableName,
                    metaBlockId,
                    blockIdsToCompact,
                    blockIdsToRemove,
                    blocksToAdd,
                    tx);

                if (mergeResults == null)
                {   //  Nothing changed
                    return false;
                }
                else
                {
                    (var newBlockBuilder, var hardDeletedRecordIds, var releasedBlockIds) =
                        mergeResults.Value;
                    var metadataTable = Database.GetAnyTable(metadataTableName);
                    var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                    var dataTableName = metaSchema.ParentSchema.TableName;

                    cumulatedHardDeletedRecordIds.Add(dataTableName, hardDeletedRecordIds);
                    cumulatedReleasedBlockIds.AddRange(releasedBlockIds);
                    if (metaBlockId == null)
                    {   //  The top of the hierarchy
                        //  Delete all in-memory meta records
                        tx.LoadCommittedBlocksInTransaction(metadataTableName);
                        
                        var tableLog = tx.TransactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[metadataTableName];

                        tableLog.CommittedDataBlock?.DeleteAll();
                        tableLog.NewDataBlock.DeleteAll();
                        tableLog.NewDataBlock.AppendBlock(newBlockBuilder);
                        //  Hard delete records
                        foreach (var p in cumulatedHardDeletedRecordIds)
                        {
                            Database.DeleteTombstoneRecords(p.Key, p.Value, tx);
                        }
                        Database.SetNoLongerInUsedBlockIds(cumulatedReleasedBlockIds, tx);

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

                        metadataTableName = metaMetaDataTableName;
                        blockIdsToRemove = [metaBlockId.Value];
                        metaBlockId = metaMetaBlockId > 0 ? metaMetaBlockId : null;
                        blockIdsToCompact = Array.Empty<int>();
                        blocksToAdd = [newBlockBuilder];
                    }
                }
            }
        }

        private (BlockBuilder NewBlock, IEnumerable<long> HardDeletedRecordIds, IEnumerable<int> ReleasedBlockIds)? MergeBlocksWithReplacementsOneLayer(
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
            (var mergedMetaBlocks, var hardDeletedRecordIds) = MergeBlockStack(
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

                foreach (var metaBlock in mergedMetaBlocks)
                {
                    newMetadataBlockBuilder.AppendRecord(
                        DateTime.Now,
                        metadataTable.NewRecordId(),
                        metaBlock.MetadataRecord.Span);
                }

                return (newMetadataBlockBuilder, hardDeletedRecordIds, removedBlockIds);
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
        private (IEnumerable<MetaDataBlock> MergeBlocks, IEnumerable<long> HardDeletedRecordIds) MergeBlockStack(
            Stack<IBlockFacade> blockStack,
            IImmutableSet<int> blockIdsToCompact,
            MetadataTableSchema metaSchema,
            TransactionContext tx)
        {
            var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
            var processedBlocks = new List<MetaDataBlock>(blockStack.Count);
            var hardDeletedRecordIds = new List<long>();

            while (blockStack.Any())
            {
                var leftBlock = blockStack.Pop();
                var leftCompaction = leftBlock.Compact(blockIdsToCompact, tx);

                if (leftCompaction != null)
                {
                    hardDeletedRecordIds.AddRange(leftCompaction.Value.HardDeletedRecordIds);
                    leftBlock = leftCompaction.Value.NewBlock;
                }
                if (leftBlock.ItemCount > 0)
                {
                    if (blockStack.Any())
                    {
                        var rightBlock = blockStack.Pop();
                        var rightCompaction = rightBlock.Compact(blockIdsToCompact, tx);

                        if (rightCompaction != null)
                        {
                            hardDeletedRecordIds.AddRange(rightCompaction.Value.HardDeletedRecordIds);
                            rightBlock = rightCompaction.Value.NewBlock;
                        }
                        if (rightBlock.ItemCount > 0)
                        {
                            if (leftBlock.ComputeSize() + rightBlock.ComputeSize() <= maxBlockSize)
                            {   //  Force compaction if it didn't happen already
                                leftCompaction = leftBlock.Compact(ImmutableHashSet<int>.Empty, tx);
                                rightCompaction = rightBlock.Compact(ImmutableHashSet<int>.Empty, tx);

                                if (leftCompaction != null)
                                {
                                    hardDeletedRecordIds.AddRange(leftCompaction.Value.HardDeletedRecordIds);
                                    leftBlock = leftCompaction.Value.NewBlock;
                                }
                                if (rightCompaction != null)
                                {
                                    hardDeletedRecordIds.AddRange(rightCompaction.Value.HardDeletedRecordIds);
                                    rightBlock = rightCompaction.Value.NewBlock;
                                }
                                if (leftBlock.ItemCount == 0 && rightBlock.ItemCount != 0)
                                {
                                    blockStack.Push(rightBlock);
                                }
                                else if (leftBlock.ItemCount != 0 && rightBlock.ItemCount == 0)
                                {
                                    blockStack.Push(leftBlock);
                                }
                                else if (leftBlock.ItemCount != 0 && rightBlock.ItemCount != 0)
                                {
                                    var resultingBlock = ((BlockBuilderFacade)leftBlock).Merge(
                                        (BlockBuilderFacade)rightBlock,
                                        tx);

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

            return (processedBlocks, hardDeletedRecordIds);
        }
        #endregion
    }
}
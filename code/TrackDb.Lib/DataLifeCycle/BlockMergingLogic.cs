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
        private record CompactResult(
            IBlockFacade NewBlock,
            IEnumerable<long> HardDeletedRecordIds);

        private record MergeResult(
            IBlockFacade NewLeftBlock,
            IBlockFacade? NewRightBlock,
            IEnumerable<long> HardDeletedRecordIds);

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
            CompactResult? CompactIf(IImmutableSet<int> blockIdsToCompact, TransactionContext tx);

            /// <summary>
            /// Try merging current and <paramref name="right"/> block.
            /// </summary>
            /// <param name="right"></param>
            /// <param name="tx"></param>
            /// <returns>
            /// <see cref="MergeResult.NewRightBlock"/> will be
            /// <c>null</c> iff:
            /// <list type="bullet">
            /// <item>
            /// Either current or <paramref name="right"/> is <see cref="MetaDataBlockFacade"/>
            /// </item>
            /// <item>If they can't be merged into a block that is persistable</item>
            /// </list>
            /// </returns>
            MergeResult TryMerge(IBlockFacade right, TransactionContext tx);

            MetaDataBlockFacade Persist(TransactionContext tx);
        }

        private record MetaDataBlockFacade(Database Database, MetaDataBlock MetaDataBlock)
            : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax() => MetaDataBlock.RecordIdMin;

            int IBlockFacade.ComputeSize() => MetaDataBlock.Size;

            int IBlockFacade.ItemCount => MetaDataBlock.ItemCount;

            CompactResult? IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                if (blockIdsToCompact.Contains(MetaDataBlock.BlockId))
                {
                    return ForceCompact(tx);
                }
                else
                {
                    return null;
                }
            }

            MergeResult IBlockFacade.TryMerge(IBlockFacade right, TransactionContext tx)
            {
                var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;

                if (((IBlockFacade)this).ComputeSize() + right.ComputeSize() <= maxBlockSize)
                {
                    var compactResult = ForceCompact(tx);
                    var mergeResult = compactResult.NewBlock.TryMerge(right, tx);

                    return new MergeResult(
                        mergeResult.NewLeftBlock,
                        mergeResult.NewRightBlock,
                        compactResult.HardDeletedRecordIds
                        .Concat(mergeResult.HardDeletedRecordIds));
                }
                else
                {
                    return new MergeResult(this, right, Array.Empty<long>());
                }
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                return this;
            }

            public CompactResult ForceCompact(TransactionContext tx)
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
                    .Filter(pf => pf.Equal(t => t.TableName, parentSchema.TableName))
                    .Filter(pf => pf.In(t => t.DeletedRecordId, allRecordIds))
                    .Select(t => t.DeletedRecordId)
                    .ToImmutableArray();
                var blockBuilder = new BlockBuilder(parentSchema);

                //  Hard delete those records
                blockBuilder.AppendBlock(block);
                blockBuilder.DeleteRecordsByRecordId(deletedRecordIds);

                return new CompactResult(
                    new BlockBuilderFacade(Database, blockBuilder),
                    deletedRecordIds);
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
                if (((IBlock)BlockBuilder).RecordCount > 0)
                {
                    var segments = BlockBuilder.SegmentRecords(
                        Database.DatabasePolicy.StoragePolicy.BlockSize);

                    return segments
                        .Sum(s => s.Size);
                }
                else
                {
                    return 0;
                }
            }

            int IBlockFacade.ItemCount => ((IBlock)BlockBuilder).RecordCount;

            CompactResult? IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                return null;
            }

            MergeResult IBlockFacade.TryMerge(IBlockFacade right, TransactionContext tx)
            {
                MergeResult MergeBlockBuilderFacades(
                    BlockBuilderFacade rightFacade,
                    IEnumerable<long> hardDeletedRecordIds,
                    TransactionContext tx)
                {
                    var newBlockBuilder =
                        new BlockBuilder(((IBlock)BlockBuilder).TableSchema);
                    IBlockFacade newFacade = new BlockBuilderFacade(Database, newBlockBuilder);

                    newBlockBuilder.AppendBlock(BlockBuilder);
                    newBlockBuilder.AppendBlock(rightFacade.BlockBuilder);

                    if (newFacade.ComputeSize() <=
                        Database.DatabasePolicy.StoragePolicy.BlockSize)
                    {
                        return new MergeResult(newFacade, null, hardDeletedRecordIds);
                    }
                    else
                    {
                        return new MergeResult(this, rightFacade, hardDeletedRecordIds);
                    }
                }

                if (((IBlockFacade)this).ComputeSize() + right.ComputeSize()
                    <= Database.DatabasePolicy.StoragePolicy.BlockSize)
                {
                    if (right is BlockBuilderFacade rbbf)
                    {
                        return MergeBlockBuilderFacades(rbbf, Array.Empty<long>(), tx);
                    }
                    else if (right is MetaDataBlockFacade rmdbf)
                    {
                        var compactResult = rmdbf.ForceCompact(tx);

                        return MergeBlockBuilderFacades(
                            (BlockBuilderFacade)compactResult.NewBlock,
                            compactResult.HardDeletedRecordIds,
                            tx);
                    }
                    else
                    {
                        throw new NotSupportedException($"{right.GetType().Name}");
                    }
                }
                else
                {
                    return new MergeResult(this, right, Array.Empty<long>());
                }
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
                var blockStats = BlockBuilder.Serialize(buffer);

                if (blockStats.Size > buffer.Length)
                {
                    throw new InvalidOperationException("Block bigger than planned");
                }

                var blockId = Database.GetAvailableBlockId(tx);
                var tableName = ((IBlock)BlockBuilder).TableSchema.TableName;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var metadataTable = tableMap[tableMap[tableName].MetaDataTableName!].Table;
                var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                var metaRecord = metaSchema.CreateMetadataRecord(blockId, blockStats);
                var metaBlock = new MetaDataBlock(metaRecord, metaSchema);

                Database.PersistBlock(blockId, buffer.AsSpan().Slice(0, blockStats.Size), tx);

                return new MetaDataBlockFacade(Database, metaBlock);
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
                        //  Replace with in-memory block
                        tableLog.NewDataBlock.AppendBlock(newBlockBuilder);

                        var prunedBlockIds = PruneMetaTable(metadataTable, tx);

                        //  Hard delete records
                        foreach (var p in cumulatedHardDeletedRecordIds)
                        {
                            Database.DeleteTombstoneRecords(p.Key, p.Value, tx);
                        }
                        Database.SetNoLongerInUsedBlockIds(
                            cumulatedReleasedBlockIds.Concat(prunedBlockIds),
                            tx);

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
                        blocksToAdd = ((IBlock)newBlockBuilder).RecordCount > 0
                            ? [newBlockBuilder]
                            : Array.Empty<BlockBuilder>();
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
                var leftCompactResult = leftBlock.CompactIf(blockIdsToCompact, tx);

                if (leftCompactResult != null)
                {
                    hardDeletedRecordIds.AddRange(leftCompactResult.HardDeletedRecordIds);
                    leftBlock = leftCompactResult.NewBlock;
                }
                if (leftBlock.ItemCount > 0)
                {
                    if (blockStack.Any())
                    {
                        var rightBlock = blockStack.Pop();
                        var rightCompactResult = rightBlock.CompactIf(blockIdsToCompact, tx);

                        if (rightCompactResult != null)
                        {
                            hardDeletedRecordIds.AddRange(rightCompactResult.HardDeletedRecordIds);
                            rightBlock = rightCompactResult.NewBlock;
                        }
                        if (rightBlock.ItemCount > 0)
                        {
                            var mergeResult = leftBlock.TryMerge(rightBlock, tx);

                            hardDeletedRecordIds.AddRange(mergeResult.HardDeletedRecordIds);
                            if (mergeResult.NewRightBlock != null)
                            {   //  No merging occured
                                processedBlocks.Add(
                                    mergeResult.NewLeftBlock.Persist(tx).MetaDataBlock);
                                blockStack.Push(mergeResult.NewRightBlock);
                            }
                            else
                            {   //  Merging occured, we keep the new block for another round
                                blockStack.Push(mergeResult.NewLeftBlock);
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

        private IEnumerable<int> PruneMetaTable(Table metaTable, TransactionContext tx)
        {
            var inMemoryRecordCount = metaTable.Query(tx)
                .WithInMemoryOnly()
                .Count();

            if (inMemoryRecordCount == 1)
            {   //  Unique block persisted
                var metaSchema = (MetadataTableSchema)metaTable.Schema;
                var inMemoryQuery = metaTable.Query(tx)
                    .WithInMemoryOnly();
                var uniqueBlockItem = inMemoryQuery
                    .Select(r => new
                    {
                        BlockId = (int)r.Span[metaSchema.BlockIdColumnIndex]!,
                        ItemCount = (int)r.Span[metaSchema.ItemCountColumnIndex]!
                    })
                    .First();

                if (uniqueBlockItem.ItemCount == 1)
                {   //  Only one record persisted in that block
                    //  it makes better sense to prune the block and have the underlying records
                    //  in RAM
                    var dataTableName = metaSchema.ParentSchema.TableName;
                    var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                    var dataTableProperties = tableMap[dataTableName];
                    var dataTable = dataTableProperties.Table;
                    var dataBlock = Database.GetOrLoadBlock(
                        uniqueBlockItem.BlockId,
                        dataTable.Schema);

                    //  Delete the meta data record
                    inMemoryQuery.Delete();
                    //  Insert record in data table
                    tx.TransactionState.UncommittedTransactionLog.AppendBlock(dataBlock);

                    //  Recurse
                    return dataTableProperties.IsMetaDataTable
                        ? PruneMetaTable(dataTable, tx)
                        .Prepend(uniqueBlockItem.BlockId)
                        : [uniqueBlockItem.BlockId];
                }
            }

            return Array.Empty<int>();
        }
    }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic : LogicBase
    {
        #region Inner Types
        private record BlockFacadeMergeResult(
            IEnumerable<MetadataBlock> MetaDataBlocks,
            IEnumerable<long> TombstoneRecordIdsToDelete);

        private record OneLayerMergeResult(
            BlockBuilder NewBlock,
            IEnumerable<int> ReleasedBlockIds,
            IEnumerable<long> TombstoneRecordIdsToDelete);

        private record TransformResult(
            bool IsTransformed,
            IEnumerable<long> TombstoneRecordIdsToDelete,
            IEnumerable<IBlockFacade> Blocks)
        {
            public static TransformResult NoTransform { get; }
                = new TransformResult(false, Array.Empty<long>(), Array.Empty<IBlockFacade>());
        }

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
            /// There might be more than one block in the case where the compacted version compresses
            /// less than original one (rare).
            /// </returns>
            TransformResult CompactIf(IImmutableSet<int> blockIdsToCompact, TransactionContext tx);

            /// <summary>
            /// Try merging current and <paramref name="right"/> block.
            /// </summary>
            /// <param name="right"></param>
            /// <param name="tx"></param>
            /// <returns>
            /// There might be more than one block in the case where the merged version compresses
            /// less than original ones (rare).
            /// </returns>
            TransformResult TryMerge(IBlockFacade right, TransactionContext tx);

            MetaDataBlockFacade Persist(TransactionContext tx);
        }

        private record MetaDataBlockFacade(Database Database, MetadataBlock MetaDataBlock)
            : IBlockFacade
        {
            long IBlockFacade.ComputeRecordIdMax() => MetaDataBlock.MinRecordId;

            int IBlockFacade.ComputeSize() => MetaDataBlock.Size;

            int IBlockFacade.ItemCount => MetaDataBlock.ItemCount;

            TransformResult IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                if (blockIdsToCompact.Contains(MetaDataBlock.BlockId))
                {
                    var blockBuilderFacade = ToBlockBuilderFacade();

                    return blockBuilderFacade.ForceCompact(tx);
                }
                else
                {
                    return TransformResult.NoTransform;
                }
            }

            TransformResult IBlockFacade.TryMerge(IBlockFacade right, TransactionContext tx)
            {
                var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;

                if (((IBlockFacade)this).ComputeSize() + right.ComputeSize() <= maxBlockSize)
                {
                    var blockBuilderFacade = ToBlockBuilderFacade();
                    var leftTransformResult = blockBuilderFacade.ForceCompact(tx);

                    return leftTransformResult.IsTransformed
                        ? new TransformResult(
                            true,
                            leftTransformResult.TombstoneRecordIdsToDelete,
                            leftTransformResult.Blocks.Append(right))
                        : new TransformResult(
                            true,
                            Array.Empty<long>(),
                            [blockBuilderFacade, right]);
                }
                else
                {
                    return TransformResult.NoTransform;
                }
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                return this;
            }

            public BlockBuilderFacade ToBlockBuilderFacade()
            {
                var parentSchema = MetaDataBlock.Schema.ParentSchema;
                var block = Database.GetOrLoadBlock(MetaDataBlock.BlockId, parentSchema);
                var blockBuilder = new BlockBuilder(parentSchema);
                var blockBuilderFacade = new BlockBuilderFacade(Database, blockBuilder);

                blockBuilder.AppendBlock(block);

                return blockBuilderFacade;
            }
        }

        private record BlockBuilderFacade(
            Database Database,
            BlockBuilder BlockBuilder,
            bool IsCompacted = false)
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
                return BlockBuilder.GetSerializationSize();
            }

            int IBlockFacade.ItemCount => ((IBlock)BlockBuilder).RecordCount;

            TransformResult IBlockFacade.CompactIf(
                IImmutableSet<int> blockIdsToCompact,
                TransactionContext tx)
            {
                return ForceCompact(tx);
            }

            TransformResult IBlockFacade.TryMerge(IBlockFacade right, TransactionContext tx)
            {
                if (((IBlockFacade)this).ComputeSize() + right.ComputeSize()
                    <= Database.DatabasePolicy.StoragePolicy.BlockSize)
                {
                    if (right is BlockBuilderFacade rbbf)
                    {
                        var newBlockBuilder =
                            new BlockBuilder(((IBlock)BlockBuilder).TableSchema);

                        newBlockBuilder.AppendBlock(BlockBuilder);
                        newBlockBuilder.AppendBlock(rbbf.BlockBuilder);

                        var blockBuilders = SegmentBlockBuilder(newBlockBuilder);
                        var facades = blockBuilders
                            .Select(s => new BlockBuilderFacade(Database, s));

                        return new TransformResult(true, Array.Empty<long>(), facades);
                    }
                    else if (right is MetaDataBlockFacade rmdbf)
                    {
                        var compactedResult = rmdbf.ToBlockBuilderFacade().ForceCompact(tx);

                        return new TransformResult(
                            true,
                            compactedResult.TombstoneRecordIdsToDelete,
                            compactedResult.Blocks.Prepend(this));
                    }
                    else
                    {
                        throw new NotSupportedException($"{right.GetType().Name}");
                    }
                }
                else
                {   //  Can't merge
                    return TransformResult.NoTransform;
                }
            }

            MetaDataBlockFacade IBlockFacade.Persist(TransactionContext tx)
            {
                var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
                var buffer = new byte[maxBlockSize];
                var blockStats = BlockBuilder.Serialize(buffer);

                if (blockStats.Size > buffer.Length)
                {
                    throw new InvalidOperationException("Block bigger than planned");
                }

                var blockId = Database.GetAvailableBlockId(tx);
                var tableName = ((IBlock)BlockBuilder).TableSchema.TableName;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var metadataTable = tableMap[tableMap[tableName].MetadataTableName!].Table;
                var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                var metaRecord = metaSchema.CreateMetadataRecord(blockId, blockStats);
                var metaBlock = new MetadataBlock(metaRecord, metaSchema);

                Database.PersistBlock(blockId, buffer.AsSpan().Slice(0, blockStats.Size), tx);

                return new MetaDataBlockFacade(Database, metaBlock);
            }

            public TransformResult ForceCompact(TransactionContext tx)
            {
                if (IsCompacted)
                {
                    return TransformResult.NoTransform;
                }
                else
                {
                    var schema = ((IBlock)BlockBuilder).TableSchema;

                    //  First let's find all record IDs in the block
                    var allRecordIds = ((IBlock)BlockBuilder).Project(
                        new object?[1],
                        [schema.RecordIdColumnIndex],
                        Enumerable.Range(0, ((IBlock)BlockBuilder).RecordCount),
                        0)
                        .Select(r => (long)r.Span[0]!)
                        .ToImmutableArray();
                    var tombstoneRecordIdColumnIndex =
                        Database.TombstoneTable.Schema.RecordIdColumnIndex;
                    var deletedRecordIdColumnIndex =
                        Database.TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId);
                    //  Then, let's find which ones are deleted
                    var tombstoneRecords = Database.TombstoneTable.Query(tx)
                        .WithCommittedOnly()
                        .Where(pf => pf.Equal(t => t.TableName, schema.TableName))
                        .Where(pf => pf.In(t => t.DeletedRecordId, allRecordIds))
                        .TableQuery
                        .WithProjection(deletedRecordIdColumnIndex.Prepend(tombstoneRecordIdColumnIndex))
                        .Select(r => new
                        {
                            TombstoneRecordId = (long)r.Span[0]!,
                            DeletedRecordId = (long)r.Span[1]!
                        })
                        .ToImmutableArray();

                    if (tombstoneRecords.Length > 0
                        || BlockBuilder.GetSerializationSize()
                        > Database.DatabasePolicy.StoragePolicy.BlockSize)
                    {   //  Delete records in the block
                        //  Take a copy of the block
                        var blockBuilderCopy = new BlockBuilder(schema);

                        blockBuilderCopy.AppendBlock(BlockBuilder);
                        blockBuilderCopy.DeleteRecordsByRecordId(
                            tombstoneRecords.Select(t => t.DeletedRecordId));

                        //  Segment resulting block builder
                        var segments = SegmentBlockBuilder(blockBuilderCopy);
                        var blocks = segments
                            .Select(s => new BlockBuilderFacade(Database, s, true))
                            .ToImmutableArray();

                        return new TransformResult(
                            true,
                            tombstoneRecords.Select(t => t.TombstoneRecordId),
                            blocks);
                    }
                    else
                    {
                        return new TransformResult(
                            true,
                            Array.Empty<long>(),
                            [this with { IsCompacted = true }]);
                    }
                }
            }

            private IEnumerable<BlockBuilder> SegmentBlockBuilder(BlockBuilder blockBuilder)
            {
                var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
                var sizes = blockBuilder.SegmentRecords(maxBlockSize);

                if (sizes.Count == 1 || ((IBlock)blockBuilder).RecordCount == 0)
                {   //  The block isn't too big
                    return [blockBuilder];
                }
                else
                {   //  We segment the block
                    var list = new List<BlockBuilder>();
                    var recordIndex = 0;
                    var totalRecordCount = ((IBlock)blockBuilder).RecordCount;
                    var schema = ((IBlock)blockBuilder).TableSchema;
                    var columnCount = schema.Columns.Count;

                    foreach (var size in sizes)
                    {
                        var subBlockBuilder = new BlockBuilder(((IBlock)blockBuilder).TableSchema);
                        var records = ((IBlock)blockBuilder).Project(
                            new object?[columnCount + 2],
                            Enumerable.Range(0, columnCount)
                            .Append(schema.RecordIdColumnIndex)
                            .Append(schema.CreationTimeColumnIndex)
                            .ToImmutableArray(),
                            Enumerable.Range(recordIndex, size.ItemCount),
                            42);

                        foreach (var record in records)
                        {
                            var recordSpan = record.Span;
                            var recordId = (long)recordSpan[columnCount]!;
                            var creationTime = (DateTime)recordSpan[columnCount + 1]!;
                            var coreRecordSpan = recordSpan.Slice(0, columnCount);

                            subBlockBuilder.AppendRecord(creationTime, recordId, coreRecordSpan);
                        }
                        recordIndex += size.ItemCount;

                        list.Add(subBlockBuilder);
                    }

                    return list;
                }
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
            var metadataTableName = tableProperties.MetadataTableName;

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
            var cummulatedTombstoneRecordIdsToDelete = new List<long>();

            while (true)
            {
                var mergeResult = MergeBlocksWithReplacementsOneLayer(
                    metadataTableName,
                    metaBlockId,
                    blockIdsToCompact,
                    blockIdsToRemove,
                    blocksToAdd,
                    tx);

                if (mergeResult == null)
                {   //  Nothing changed
                    return false;
                }
                else
                {
                    var metadataTable = Database.GetAnyTable(metadataTableName);
                    var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                    var dataTableName = metaSchema.ParentSchema.TableName;

                    cummulatedTombstoneRecordIdsToDelete.AddRange(
                        mergeResult.TombstoneRecordIdsToDelete);
                    //  Release block ids
                    Database.SetNoLongerInUsedBlockIds(mergeResult.ReleasedBlockIds, tx);

                    if (metaBlockId == null)
                    {   //  The top of the hierarchy
                        //  Delete all committed in-memory meta records
                        tx.LoadCommittedBlocksInTransaction(metadataTableName);

                        var tableLog = tx.TransactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[metadataTableName];

                        //  Here we flip the old to the new representation (merged blocks)
                        tableLog.CommittedDataBlock!.DeleteAll();
                        //  Replace with in-memory block & in-place
                        //  In-place:  this isn't new data, just new representation
                        tableLog.CommittedDataBlock!.AppendBlock(mergeResult.NewBlock);

                        var prunedBlockIds = PruneHeadMetaTable(metadataTable, tx);

                        if (prunedBlockIds.Any())
                        {
                            Database.SetNoLongerInUsedBlockIds(prunedBlockIds, tx);
                        }
                        Database.CheckAvailabilityDuplicates(tx);
                        HardDeleteTombstoneRecordIds(cummulatedTombstoneRecordIdsToDelete, tx);
                        Database.CheckAvailabilityDuplicates(tx);

                        return true;
                    }
                    else
                    {   //  Climb the hierarchy
                        var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                        var metaMetaDataTableName = tableMap[metadataTableName].MetadataTableName!;
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
                        blocksToAdd = ((IBlock)mergeResult.NewBlock).RecordCount > 0
                            ? [mergeResult.NewBlock]
                            : Array.Empty<BlockBuilder>();
                    }
                }
            }
        }

        private void HardDeleteTombstoneRecordIds(
            IEnumerable<long> tombstoneRecordIdsToDelete,
            TransactionContext tx)
        {
            if (tombstoneRecordIdsToDelete.Any())
            {
                tx.LoadCommittedBlocksInTransaction(Database.TombstoneTable.Schema.TableName);
                tx.TransactionState
                    .UncommittedTransactionLog
                    .TransactionTableLogMap[Database.TombstoneTable.Schema.TableName]
                    .CommittedDataBlock!.DeleteRecordsByRecordId(tombstoneRecordIdsToDelete);
            }
        }

        private OneLayerMergeResult? MergeBlocksWithReplacementsOneLayer(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            IEnumerable<int> blockIdsToRemove,
            IEnumerable<BlockBuilder> blocksToAdd,
            TransactionContext tx)
        {
            (var blockFacades, var originalBlockIds) = LoadBlockFacades(
                metadataTableName,
                metaBlockId,
                blockIdsToRemove,
                blocksToAdd,
                tx);
            var metadataTable = Database.GetAnyTable(metadataTableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var mergedResult = MergeBlockAlgorithm(
                blockFacades,
                blockIdsToCompact,
                metaSchema,
                tx);
            var newBlockIds = mergedResult.MetaDataBlocks
                .Select(m => m.BlockId)
                .ToImmutableArray();
            var removedBlockIds = originalBlockIds
                .Except(newBlockIds)
                .ToImmutableArray();

            if (removedBlockIds.Any())
            {
                var newMetadataBlockBuilder = new BlockBuilder(metaSchema);

                foreach (var metaBlock in mergedResult.MetaDataBlocks)
                {
                    newMetadataBlockBuilder.AppendRecord(
                        DateTime.Now,
                        metadataTable.NewRecordId(),
                        metaBlock.MetadataRecord.Span);
                }

                return new OneLayerMergeResult(
                    newMetadataBlockBuilder,
                    removedBlockIds,
                    mergedResult.TombstoneRecordIdsToDelete);
            }
            else
            {
                return null;
            }
        }

        #region Block loading
        private IImmutableList<MetadataBlock> LoadMetaDataBlocks(
            string metadataTableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            if (metaBlockId != null)
            {
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var metaMetadataTableName = tableMap[metadataTableName].MetadataTableName;
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
                    .Select(r => new MetadataBlock(r.ToArray(), metadataTableSchema))
                    .ToImmutableArray();

                return metaDataBlocks;
            }
            else
            {
                var metadataTable = Database.GetAnyTable(metadataTableName);
                var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
                var metaDataBlocks = metadataTable.Query(tx)
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithCommittedOnly()
                    .WithInMemoryOnly()
                    .Select(r => new MetadataBlock(r.ToArray(), metadataTableSchema))
                    .ToImmutableArray();

                return metaDataBlocks;
            }
        }

        private (IEnumerable<IBlockFacade> BlockFacades, IEnumerable<int> OriginalBlockIds) LoadBlockFacades(
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

            return (allFacades.ToImmutableArray(), originalBlockIds);
        }
        #endregion

        #region Merge algorithm
        private BlockFacadeMergeResult MergeBlockAlgorithm(
            IEnumerable<IBlockFacade> blocks,
            IEnumerable<int> blockIdsToCompact,
            MetadataTableSchema metaSchema,
            TransactionContext tx)
        {
            var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
            var blockStack = new Stack<IBlockFacade>(blocks.Reverse());
            var blockIdsToCompactSet = blockIdsToCompact.ToImmutableHashSet();
            var processedBlocks = new List<MetadataBlock>(blockStack.Count);
            var cummulatedTombstoneRecordIdsToDelete = new List<long>();

            void PushRange(IEnumerable<IBlockFacade> blocks)
            {
                foreach (var block in blocks)
                {
                    blockStack.Push(block);
                }
            }

            while (blockStack.Any())
            {
                var leftBlock = blockStack.Pop();
                var compactedLeftBlocks = leftBlock.CompactIf(blockIdsToCompactSet, tx);

                if (compactedLeftBlocks.IsTransformed)
                {
                    cummulatedTombstoneRecordIdsToDelete.AddRange(
                        compactedLeftBlocks.TombstoneRecordIdsToDelete);
                    PushRange(compactedLeftBlocks.Blocks);
                }
                else if (leftBlock.ItemCount > 0)
                {
                    if (blockStack.Any())
                    {
                        var rightBlock = blockStack.Pop();
                        var compactedRightBlocks = rightBlock.CompactIf(blockIdsToCompactSet, tx);

                        if (compactedRightBlocks.IsTransformed)
                        {
                            cummulatedTombstoneRecordIdsToDelete.AddRange(
                                compactedRightBlocks.TombstoneRecordIdsToDelete);
                            blockStack.Push(leftBlock);
                            PushRange(compactedRightBlocks.Blocks);
                        }
                        else if (rightBlock.ItemCount > 0)
                        {
                            var mergedResult = leftBlock.TryMerge(rightBlock, tx);

                            if (mergedResult.IsTransformed)
                            {
                                cummulatedTombstoneRecordIdsToDelete.AddRange(
                                    mergedResult.TombstoneRecordIdsToDelete);
                                PushRange(mergedResult.Blocks);
                            }
                            else
                            {   //  No merge occured
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

            return new BlockFacadeMergeResult(processedBlocks, cummulatedTombstoneRecordIdsToDelete);
        }
        #endregion

        private IEnumerable<int> PruneHeadMetaTable(Table metaTable, TransactionContext tx)
        {
            var inMemoryRecordCount = metaTable.Query(tx)
                .Count();
            var inMemoryCommittedRecordCount = metaTable.Query(tx)
                .WithCommittedOnly()
                .Count();

            //  We prune only if there is a unique block and it's committed
            if (inMemoryRecordCount == 1 && inMemoryCommittedRecordCount == 1)
            {   //  Unique block persisted
                var metaSchema = (MetadataTableSchema)metaTable.Schema;
                var metaTableName = metaSchema.TableName;

                tx.LoadCommittedBlocksInTransaction(metaTableName);

                var committedDataBlock = tx.TransactionState
                    .UncommittedTransactionLog
                    .TransactionTableLogMap[metaTableName]
                    .CommittedDataBlock!;
                var uniqueBlockItem = ((IBlock)committedDataBlock).Project(
                    new object?[2],
                    [metaSchema.BlockIdColumnIndex, metaSchema.ItemCountColumnIndex],
                    [0],
                    0)
                    .Select(r => new
                    {
                        BlockId = (int)r.Span[0]!,
                        ItemCount = (int)r.Span[1]!
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
                    var transactionTableLogMap = tx.TransactionState
                        .UncommittedTransactionLog
                        .TransactionTableLogMap;

                    tx.LoadCommittedBlocksInTransaction(dataTableName);
                    if (!transactionTableLogMap.ContainsKey(dataTableName))
                    {
                        transactionTableLogMap.Add(
                            dataTableName,
                            new TransactionTableLog(
                                new BlockBuilder(dataTable.Schema),
                                new BlockBuilder(dataTable.Schema)));
                    }
                    //  Delete the meta data record in-place (in committed blocks)
                    committedDataBlock.DeleteAll();
                    //  Insert record in data table, in-place
                    transactionTableLogMap[dataTableName]
                        .CommittedDataBlock!
                        .AppendBlock(dataBlock);

                    //  Recurse
                    return dataTableProperties.IsMetaDataTable
                        ? PruneHeadMetaTable(dataTable, tx)
                        .Prepend(uniqueBlockItem.BlockId)
                        : [uniqueBlockItem.BlockId];
                }
            }

            return Array.Empty<int>();
        }
    }
}
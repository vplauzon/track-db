using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic : LogicBase
    {
        #region Inner Types
        private record CompactionResult(
            IEnumerable<MetadataBlock> MetadataBlocks,
            IEnumerable<long> HardDeletedRecordIds);
        #endregion

        private readonly int _maxBlockSize;
        private readonly MetaBlockManager _metaBlockManager;

        public BlockMergingLogic(Database database, MetaBlockManager metaBlockManager)
            : base(database)
        {
            _maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
            _metaBlockManager = metaBlockManager;
        }

        /// <summary>
        /// Merge together blocks belonging to <paramref name="metaBlockId"/>.
        /// If <paramref name="tableName"/> is a data table, i.e. is at the lowest level,
        /// it will compact tombstoned records.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="metaBlockId"></param>
        /// <returns></returns>
        public bool CompactMerge(string tableName, int? metaBlockId)
        {
            var tx = _metaBlockManager.Tx;
            var originalBlocks = _metaBlockManager.LoadBlocks(tableName, metaBlockId);
            var compactionResult = CompactMergeBlocks(tableName, originalBlocks);
            var removedBlockIds = originalBlocks
                .Select(b => b.BlockId)
                .Except(compactionResult.MetadataBlocks.Select(b => b.BlockId))
                .ToImmutableArray();

            if (!removedBlockIds.Any() && compactionResult.HardDeletedRecordIds.Any())
            {
                throw new InvalidOperationException("No altered block but some hard deleted records");
            }
            if (!removedBlockIds.Any())
            {
                return false;
            }
            else
            {
#if DEBUG
                var countBefore = Database.GetAnyTable(tableName).Query(tx).Count();
#endif
                if (removedBlockIds.Length > 0)
                {   //  It could be zero if we only have phantom tombstones
                    Database.SetNoLongerInUsedBlockIds(removedBlockIds, tx);
                }

                ReplaceMetaBlockInHierarchy(
                    tableName,
                    metaBlockId,
                    compactionResult.MetadataBlocks);

                //  This is done after so we do not mess with queries during the processing
                Database.DeleteTombstoneRecords(
                    tableName,
                    compactionResult.HardDeletedRecordIds,
                    tx);

#if DEBUG
                var countAfter = Database.GetAnyTable(tableName).Query(tx).Count();

                if (countBefore != countAfter)
                {
                    throw new InvalidOperationException("Inconsistent merge");
                }
#endif

                return true;
            }
        }

        #region Post Merge
        private void ReplaceMetaBlockInHierarchy(
            string tableName,
            int? metaBlockId,
            IEnumerable<MetadataBlock> metadataBlocks)
        {   //  Build a block with the meta data blocks
            var tx = _metaBlockManager.Tx;
            var metaTable = Database.GetMetaDataTable(tableName);
            var metaSchema = metaTable.Schema;
            var metaBuilder = ToBlockBuilder(metadataBlocks, metaTable);

            //  Replace that
            ReplaceMetaBlockInHierarchy(metaSchema.TableName, metaBlockId, metaBuilder);
        }

        private static BlockBuilder ToBlockBuilder(IEnumerable<MetadataBlock> metadataBlocks, Table metaTable)
        {
            var metaSchema = metaTable.Schema;
            var metaBuilder = new BlockBuilder(metaSchema);

            foreach (var metadataBlock in metadataBlocks)
            {
                var metadataSpan = metadataBlock.MetadataRecord.Span;
                var recordId = metaTable.NewRecordId();
                var recordSpan = metadataSpan.Slice(0, metaSchema.Columns.Count);

                metaBuilder.AppendRecord(DateTime.Now, recordId, metadataSpan);
            }

            return metaBuilder;
        }

        private void ReplaceMetaBlockInHierarchy(
            string metaTableName,
            int? metaBlockId,
            BlockBuilder metaBuilder)
        {
            if (((IBlock)metaBuilder).TableSchema.TableName != metaTableName)
            {
                throw new ArgumentException(nameof(metaBuilder));
            }
            if (metaBlockId == null)
            {   //  There are no meta-meta block containing the meta blocks
                //  We simply replace in-memory meta blocks
                _metaBlockManager.ReplaceInMemoryBlocks(metaTableName, metaBuilder);
            }
            else if (metaBlockId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(metaBlockId));
            }
            else
            {
                var metaMetaTable = Database.GetMetaDataTable(metaTableName);
                var metaMetaSchema = (MetadataTableSchema)metaMetaTable.Schema;
                var metaMetaBlockId = _metaBlockManager.GetMetaBlockId(
                    metaMetaTable.Schema.TableName,
                    metaBlockId.Value);
                var metaMetaBlocks = _metaBlockManager.LoadBlocks(metaTableName, metaMetaBlockId);

                Database.SetNoLongerInUsedBlockIds([metaBlockId.Value], _metaBlockManager.Tx);
                if (!metaMetaBlocks.Any())
                {
                    throw new InvalidOperationException(
                        $"Block ID {metaMetaBlockId} on table '{metaTableName}' has not data");
                }
                if (metaMetaBlocks.First().Schema.TableName != metaMetaSchema.TableName)
                {
                    throw new InvalidOperationException("Inconsistant schema");
                }
                if (metaMetaBlocks.Where(mmb => mmb.BlockId == metaBlockId.Value).Count() != 1)
                {
                    throw new InvalidOperationException(
                        $"Block ID {metaMetaBlockId} on table '{metaTableName}' doesn't " +
                        $"contain block {metaBlockId}");
                }
                if (((IBlock)metaBuilder).RecordCount > 0)
                {
                    CompactMetaBlock(
                        metaBlockId.Value,
                        metaBuilder,
                        metaMetaTable,
                        metaMetaSchema,
                        metaMetaBlockId,
                        metaMetaBlocks);
                }
                else
                {   //  Empty metaBuilder
                    var remainingMetaMetaBlocks = metaMetaBlocks
                        //  Remove the current that got modified
                        .Where(mmb => mmb.BlockId != metaBlockId.Value);
                    var metaMetaBuilder = ToBlockBuilder(remainingMetaMetaBlocks, metaMetaTable);

                    ReplaceMetaBlockInHierarchy(metaMetaSchema.TableName, metaMetaBlockId, metaMetaBuilder);
                }
            }
        }

        private void CompactMetaBlock(
            int metaBlockId,
            BlockBuilder metaBuilder,
            Table metaMetaTable,
            MetadataTableSchema metaMetaSchema,
            int? metaMetaBlockId,
            IEnumerable<MetadataBlock> metaMetaBlocks)
        {
            var minRecordId = ((IBlock)metaBuilder).Project(
                new object?[1],
                [((IBlock)metaBuilder).TableSchema.RecordIdColumnIndex],
                Enumerable.Range(0, ((IBlock)metaBuilder).RecordCount),
                0)
                .Max(r => (long)r.Span[0]!);
            var orderedMetaMetaBlocks = metaMetaBlocks
                //  Remove the current that got modified
                .Where(mmb => mmb.BlockId != metaBlockId)
                .OrderBy(mmb => Math.Abs(mmb.MinRecordId - minRecordId));
            var stack = new Stack<MetadataBlock>(orderedMetaMetaBlocks.Reverse());
            var procesedMetaBlocks = new List<MetadataBlock>(stack.Count + 1);
            var tombstonedRecordIds = ImmutableArray<long>.Empty;
            var hardDeleteRecordIds = new List<long>();

            //  Try to merge the new block as much as possible
            while (stack.Any())
            {
                var metaMetaBlock = stack.Pop();

                if (!TryMerge(
                    metaBuilder,
                    metaMetaBlock,
                    tombstonedRecordIds,
                    hardDeleteRecordIds))
                {   //  We are complete
                    procesedMetaBlocks.Add(metaMetaBlock);
                    procesedMetaBlocks.AddRange(stack);
                    stack.Clear();
                }
            }
            //  Persist that new block into the meta blocks list
            PersistBlockBuilder(metaBuilder, true, procesedMetaBlocks, metaMetaSchema);

            var metaMetaBuilder = ToBlockBuilder(procesedMetaBlocks, metaMetaTable);

            ReplaceMetaBlockInHierarchy(metaMetaSchema.TableName, metaMetaBlockId, metaMetaBuilder);
        }
        #endregion

        #region Merge logic
        private CompactionResult CompactMergeBlocks(
            string tableName,
            IEnumerable<MetadataBlock> blocks)
        {
            var tx = _metaBlockManager.Tx;
            var schema = Database.GetAnyTable(tableName).Schema;
            var metadataTable = Database.GetMetaDataTable(tableName);
            var metadataSchema = (MetadataTableSchema)metadataTable.Schema;
            var blockStack = new Stack<MetadataBlock>(blocks.OrderBy(b => b.MinRecordId));
            var processedBlocks = new List<MetadataBlock>(2 * blockStack.Count());
            var hardDeletedRecordIds = new List<long>();
            var blockBuilder = new BlockBuilder(schema);
            MetadataBlock? previousBlock = null;
            var tombstoneBaseQuery = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, schema.TableName));

            while (blockStack.Count > 0)
            {
                var currentBlock = blockStack.Pop();
                var blockTombstonedRecordIds = tombstoneBaseQuery
                    .Where(pf => pf.GreaterThanOrEqual(t => t.DeletedRecordId, currentBlock.MinRecordId)
                    .And(pf.LessThanOrEqual(t => t.DeletedRecordId, currentBlock.MaxRecordId)))
                    .TableQuery
                    .WithProjection(Database.TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId))
                    .Select(r => (long)r.Span[0]!)
                    .ToImmutableArray();

                CompactMergeOneBlock(
                    currentBlock,
                    metadataSchema,
                    blockTombstonedRecordIds,
                    blockBuilder,
                    ref previousBlock,
                    processedBlocks,
                    hardDeletedRecordIds);
            }
            if (previousBlock != null)
            {
                processedBlocks.Add(previousBlock);
            }
            //  Flush whatever remains
            PersistBlockBuilder(
                blockBuilder,
                true,
                processedBlocks,
                metadataSchema);

            return new CompactionResult(processedBlocks, hardDeletedRecordIds);
        }

        private void CompactMergeOneBlock(
            MetadataBlock currentBlock,
            MetadataTableSchema metaSchema,
            IImmutableList<long> blockTombstonedRecordIds,
            BlockBuilder blockBuilder,
            ref MetadataBlock? previousBlock,
            List<MetadataBlock> processedBlocks,
            List<long> hardDeletedRecordIds)
        {
            var tx = _metaBlockManager.Tx;

            if (blockTombstonedRecordIds.Count > 0)
            {
                if (previousBlock != null)
                {
                    processedBlocks.Add(previousBlock);
                    previousBlock = null;
                }
                CompactBlock(
                    currentBlock,
                    blockBuilder,
                    blockTombstonedRecordIds,
                    hardDeletedRecordIds);
                //  Persist the head of the block if there is too much data to fit in a block
                PersistBlockBuilder(blockBuilder, false, processedBlocks, metaSchema);
            }
            else if (previousBlock == null)
            {
                if (((IBlock)blockBuilder).RecordCount == 0)
                {
                    previousBlock = currentBlock;
                }
                else
                {
                    var isMerged = TryMerge(
                        blockBuilder,
                        currentBlock,
                        blockTombstonedRecordIds,
                        hardDeletedRecordIds);

                    if (!isMerged)
                    {   //  Can't merge
                        PersistBlockBuilder(blockBuilder, true, processedBlocks, metaSchema);
                        previousBlock = currentBlock;
                    }
                }
            }
            else
            {   //  previousBlock != null
                var isMerged = TryMerge(
                    blockBuilder,
                    previousBlock,
                    currentBlock,
                    blockTombstonedRecordIds,
                    hardDeletedRecordIds);

                if (!isMerged)
                {   //  Can't merge
                    processedBlocks.Add(previousBlock);
                    previousBlock = currentBlock;
                }
                else
                {
                    previousBlock = null;
                }
            }
        }

        private void PersistBlockBuilder(
            BlockBuilder blockBuilder,
            bool persistAll,
            List<MetadataBlock> processedBlocks,
            MetadataTableSchema metaSchema)
        {
            var tx = _metaBlockManager.Tx;

            if (((IBlock)blockBuilder).RecordCount != 0)
            {
                blockBuilder.OrderByRecordId();

                var sizes = blockBuilder.SegmentRecords(_maxBlockSize);

                if (persistAll || sizes.Count > 1)
                {
                    var actualSizes = persistAll
                        ? sizes
                        : sizes.SkipLast(1);
                    var buffer = new byte[_maxBlockSize];
                    var skipRows = 0;

                    foreach (var size in actualSizes)
                    {
                        var blockStats = blockBuilder.Serialize(buffer, skipRows, size.ItemCount);
                        var actualBuffer = buffer.AsSpan().Slice(0, blockStats.Size);
                        var blockId = Database.GetAvailableBlockId(tx);
                        var metadataRecord = metaSchema.CreateMetadataRecord(blockId, blockStats);

                        if (blockStats.Size != size.Size)
                        {
                            throw new InvalidOperationException(
                                $"Mismatch between predicted size ({size.Size}) and serialized" +
                                $"size ({blockStats.Size})");
                        }
                        Database.PersistBlock(blockId, actualBuffer, tx);
                        processedBlocks.Add(new MetadataBlock(metadataRecord, metaSchema));
                        skipRows += size.ItemCount;
                    }
                    if (skipRows == ((IBlock)blockBuilder).RecordCount)
                    {   //  Optimization as DeleteAll is more efficient than DeleteRecordsByRecordIndex
                        blockBuilder.DeleteAll();
                    }
                    else
                    {
                        blockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(0, skipRows));
                    }
                }
            }
        }

        #region Merge block operations
        private bool TryMerge(
            BlockBuilder blockBuilder,
            MetadataBlock nextMetadataBlock,
            IImmutableList<long> blockTombstonedRecordIds,
            List<long> hardDeletedRecordIds)
        {
            var tx = _metaBlockManager.Tx;
            var totalSize = blockBuilder.GetSerializationSize() + nextMetadataBlock.Size;

#if DEBUG
            if (Database.GetMetaDataTable(((IBlock)blockBuilder).TableSchema.TableName).Schema.TableName
                != nextMetadataBlock.Schema.TableName)
            {
                throw new InvalidOperationException("Inconsistant schema");
            }
#endif
            if (totalSize <= _maxBlockSize)
            {
                var recordCountBefore = ((IBlock)blockBuilder).RecordCount;
                var nextBlock = Database.GetOrLoadBlock(
                    nextMetadataBlock.BlockId,
                    nextMetadataBlock.Schema.ParentSchema);

                blockBuilder.AppendBlock(nextBlock);

                var actuallyDeletedRecordIds =
                    blockBuilder.DeleteRecordsByRecordId(blockTombstonedRecordIds);

                if (blockBuilder.GetSerializationSize() <= _maxBlockSize)
                {
                    hardDeletedRecordIds.AddRange(actuallyDeletedRecordIds);

                    return true;
                }
                else
                {
                    blockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(
                        recordCountBefore,
                        ((IBlock)blockBuilder).RecordCount - recordCountBefore));

                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        private bool TryMerge(
            BlockBuilder blockBuilder,
            MetadataBlock metadataBlockA,
            MetadataBlock metadataBlockB,
            IImmutableList<long> blockTombstonedRecordIds,
            List<long> hardDeletedRecordIds)
        {
            var tx = _metaBlockManager.Tx;

            if (((IBlock)blockBuilder).RecordCount != 0)
            {
                throw new ArgumentException(nameof(blockBuilder));
            }

            var totalSize = metadataBlockA.Size + metadataBlockB.Size;

            if (totalSize <= _maxBlockSize)
            {
                var blockA = Database.GetOrLoadBlock(
                    metadataBlockA.BlockId,
                    metadataBlockA.Schema.ParentSchema);
                var blockB = Database.GetOrLoadBlock(
                    metadataBlockA.BlockId,
                    metadataBlockA.Schema.ParentSchema);

                blockBuilder.AppendBlock(blockA);
                blockBuilder.AppendBlock(blockB);

                var actuallyDeletedRecordIds =
                    blockBuilder.DeleteRecordsByRecordId(blockTombstonedRecordIds);

                if (blockBuilder.GetSerializationSize() <= _maxBlockSize)
                {
                    hardDeletedRecordIds.AddRange(actuallyDeletedRecordIds);

                    return true;
                }
                else
                {
                    blockBuilder.DeleteAll();

                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        #endregion

        private void CompactBlock(
            MetadataBlock metadataBlock,
            BlockBuilder blockBuilder,
            IEnumerable<long> blockTombstonedRecordIds,
            List<long> hardDeletedRecordIds)
        {
            var tx = _metaBlockManager.Tx;
            var schema = ((IBlock)blockBuilder).TableSchema;
            var block = Database.GetOrLoadBlock(metadataBlock.BlockId, schema);

            blockBuilder.AppendBlock(block);

            var actuallyDeletedRecordIds =
                blockBuilder.DeleteRecordsByRecordId(blockTombstonedRecordIds);

            hardDeletedRecordIds.AddRange(actuallyDeletedRecordIds);
        }
        #endregion
    }
}
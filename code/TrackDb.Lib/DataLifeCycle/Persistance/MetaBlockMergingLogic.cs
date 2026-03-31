using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection.Metadata;
using TrackDb.Lib.DataLifeCycle;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class MetaBlockMergingLogic : LogicBase
    {
        private readonly ushort _maxBlockSize;

        public MetaBlockMergingLogic(Database database)
            : base(database)
        {
            _maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
        }

        public (MetadataBlock? MetaMetadataBlock, IEnumerable<int> DeletedBlockIds) CompactMerge(
            int metaBlockId,
            TableSchema schema,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var blockIdsToCompactSet = blockIdsToCompact.ToHashSet();
            var metaBlockManager = new MetaBlockManager(Database, tx);
            var metadataTable = Database.GetMetaDataTable(schema.TableName);
            var metadataSchema = (MetadataTableSchema)metadataTable.Schema;
            var blocks = metaBlockManager.LoadBlocks(
                schema.TableName,
                metaBlockId <= 0 ? null : metaBlockId);
            var processedBlocks = CompactMergeBlocks(
                blocks,
                schema,
                metadataSchema,
                blockIdsToCompactSet,
                allTombstoneBlockIndex,
                metaBlockManager);
            var deletedBlockIds = blocks
                .Select(b => b.BlockId)
                .Except(processedBlocks.Select(b => b.BlockId))
                .ToArray();
            var metaMetadataBlock = processedBlocks.Count > 0
                ? PersistMetaBlocks(metadataSchema, processedBlocks, metaBlockManager)
                : null;

            return (metaMetadataBlock, deletedBlockIds);
        }

        private IReadOnlyCollection<MetadataBlock> CompactMergeBlocks(
            IEnumerable<MetadataBlock> blocks,
            TableSchema schema,
            MetadataTableSchema metadataSchema,
            ISet<int> blockIdsToCompactSet,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            MetaBlockManager metaBlockManager)
        {
            var blockStack = new Stack<MetadataBlock>(blocks.OrderByDescending(b => b.MinRecordId));
            var processedBlocks = new List<MetadataBlock>(2 * blockStack.Count());
            var blockBuilder = new BlockBuilder(schema);
            MetadataBlock? previousBlock = null;

            while (blockStack.Count > 0)
            {
                var currentBlock = blockStack.Pop();

                CompactMergeOneBlock(
                    currentBlock,
                    metadataSchema,
                    blockBuilder,
                    ref previousBlock,
                    processedBlocks,
                    blockIdsToCompactSet,
                    allTombstoneBlockIndex,
                    metaBlockManager);
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
                metadataSchema,
                metaBlockManager);

            return processedBlocks;
        }

        private void CompactMergeOneBlock(
           MetadataBlock currentBlock,
           MetadataTableSchema metaSchema,
           BlockBuilder blockBuilder,
           ref MetadataBlock? previousBlock,
           List<MetadataBlock> processedBlocks,
           ISet<int> blockIdsToCompact,
           IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
           MetaBlockManager metaBlockManager)
        {
            var tx = metaBlockManager.Tx;

            if (((IBlock)blockBuilder).RecordCount > 0 && previousBlock != null)
            {
                throw new InvalidOperationException("Both blocks can't exist at the same time");
            }

            //  We try to compact regardless if the blockBuilder has records or not
            if (blockIdsToCompact.Contains(currentBlock.BlockId))
            {
                CompactBlock(
                    currentBlock,
                    blockBuilder,
                    allTombstoneBlockIndex,
                    metaBlockManager);
                //  Removing rows increases block size (rare)
                PersistBlockBuilder(
                    blockBuilder,
                    false,
                    processedBlocks,
                    metaSchema,
                    metaBlockManager);
                if (previousBlock != null)
                {
                    //  Try to merge previous block with compacted block
                    if (((IBlock)blockBuilder).RecordCount > 0 && TryMerge(
                        blockBuilder,
                        previousBlock,
                        allTombstoneBlockIndex,
                        metaBlockManager))
                    {
                        //  All good
                    }
                    else
                    {
                        processedBlocks.Add(previousBlock);
                    }
                    previousBlock = null;
                }
            }
            //  Try to merge with previous in-memory block with current block
            else if (((IBlock)blockBuilder).RecordCount > 0 && TryMerge(
                blockBuilder,
                currentBlock,
                allTombstoneBlockIndex,
                metaBlockManager))
            {
                //  All good
            }
            else
            {   //  Persist the builder completly
                PersistBlockBuilder(
                    blockBuilder,
                    true,
                    processedBlocks,
                    metaSchema,
                    metaBlockManager);
                if (previousBlock != null)
                {
                    processedBlocks.Add(previousBlock);
                }
                previousBlock = currentBlock;
            }
        }

        private void CompactBlock(
            MetadataBlock currentBlock,
            BlockBuilder blockBuilder,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            MetaBlockManager metaBlockManager)
        {
            var tx = metaBlockManager.Tx;
            var schema = ((IBlock)blockBuilder).TableSchema;
            var block = Database.GetOrLoadBlock(currentBlock.BlockId, schema);
            var recordCountBefore = ((IBlock)blockBuilder).RecordCount;
            var tombstoneRowIndexes = allTombstoneBlockIndex[currentBlock.BlockId]
                .RowIndexes
                .Distinct()
                .OrderBy(x => x)
                .Select(x => x + recordCountBefore)
                .ToArray();

            if (tombstoneRowIndexes.Length != currentBlock.ItemCount)
            {   //  Partial delete
                blockBuilder.AppendBlock(block);
                blockBuilder.DeleteRecordsByRecordIndex(tombstoneRowIndexes);
            }
        }
        private bool TryMerge(
            BlockBuilder blockBuilder,
            MetadataBlock nextMetadataBlock,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            MetaBlockManager metaBlockManager)
        {
            var tx = metaBlockManager.Tx;
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
                var tombstoneRowIndexes = allTombstoneBlockIndex[nextMetadataBlock.BlockId]
                    .RowIndexes
                    .Distinct()
                    .OrderBy(x => x)
                    .Select(x => x + recordCountBefore)
                    .ToArray();
                var nextBlock = Database.GetOrLoadBlock(
                    nextMetadataBlock.BlockId,
                    nextMetadataBlock.Schema.ParentSchema);

                blockBuilder.AppendBlock(nextBlock);
                blockBuilder.DeleteRecordsByRecordIndex(tombstoneRowIndexes);

                if (blockBuilder.GetSerializationSize() <= _maxBlockSize)
                {
                    return true;
                }
                else
                {
                    blockBuilder.DeleteRecordsByRecordIndex(
                        Enumerable.Range(0, ((IBlock)blockBuilder).RecordCount)
                        .Where(i => i >= recordCountBefore));

                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        private void PersistBlockBuilder(
            BlockBuilder blockBuilder,
            bool persistAll,
            List<MetadataBlock> processedBlocks,
            MetadataTableSchema metaSchema,
            MetaBlockManager metaBlockManager)
        {
            if (((IBlock)blockBuilder).RecordCount > 0)
            {
                var tx = metaBlockManager.Tx;

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
                        var blockId = Database.AvailabilityBlockManager.SetInUse(1, tx)[0];
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
                        blockBuilder.Clear();
                    }
                    else
                    {
                        blockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(0, skipRows));
                    }
                }
            }
        }

        private MetadataBlock PersistMetaBlocks(
            MetadataTableSchema metadataSchema,
            IReadOnlyCollection<MetadataBlock> processedBlocks,
            MetaBlockManager metaBlockManager)
        {
            var metaMetaBlockBuilder = new BlockBuilder(metadataSchema, processedBlocks.Count);
            var metaTable = Database.GetAnyTable(metadataSchema.TableName);
            var metaMetaTable = Database.GetMetaDataTable(metadataSchema.TableName);
            var metaMetaSchema = (MetadataTableSchema)metaMetaTable.Schema;
            var recordIds = metaTable.NewRecordIds(processedBlocks.Count);
            var pairs = processedBlocks
                .Select(b => b.MetadataRecord)
                .Zip(recordIds, (r, id) => new { Record = r, RecordId = id });
            var newBlockId = Database.AvailabilityBlockManager.SetInUse(1, metaBlockManager.Tx)[0];

            foreach (var pair in pairs)
            {
                metaMetaBlockBuilder.AppendRecord(pair.RecordId, pair.Record.Span);
            }

            var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
            var blockStats = metaMetaBlockBuilder.Serialize(buffer, 0, processedBlocks.Count);
            var metaMetadataRecord = metaMetaSchema.CreateMetadataRecord(newBlockId, blockStats);
            var metaMetadataBlock = new MetadataBlock(metaMetadataRecord, metaMetaSchema);

            Database.PersistBlock(newBlockId, buffer, metaBlockManager.Tx);

            return metaMetadataBlock;
        }
    }
}
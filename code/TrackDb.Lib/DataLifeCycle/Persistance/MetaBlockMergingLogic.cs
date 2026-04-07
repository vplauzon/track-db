using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class MetaBlockMergingLogic : LogicBase
    {
        #region Inner Types
        public record CompactMergeResult(
            //  Deleted block IDs
            IEnumerable<int> DeletedBlockIds,
            //  Meta blocks, available only if persistance was requested
            IEnumerable<MetadataBlock> MetaBlocks);
        #endregion

        private readonly ushort _maxBlockSize;

        public MetaBlockMergingLogic(Database database)
            : base(database)
        {
            _maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;
        }

        public CompactMergeResult CompactMerge(
            int? metaBlockId,
            MetadataTableSchema schema,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            IDictionary<int, IEnumerable<MetadataBlock>> blockReplacementMap,
            TransactionContext tx)
        {
            var blocks = LoadBlocks(schema.TableName, metaBlockId, tx);
            var replacedBlocks = blocks
                .SelectMany(b => blockReplacementMap.TryGetValue(b.BlockId, out var replacements)
                ? replacements
                : [b])
                .ToArray();
            var processedBlocks = CompactMergeBlocks(
                replacedBlocks,
                schema,
                blockIdsToCompact,
                allTombstoneBlockIndex,
                tx);
            var deletedBlockIds = replacedBlocks
                .Select(b => b.BlockId)
                .Except(processedBlocks.Select(b => b.BlockId))
                .ToArray();
            var metaMetaBlocks = PersistMetaBlocks(metaBlockId, schema, processedBlocks, tx);

            return new(deletedBlockIds, metaMetaBlocks);
        }

        private IReadOnlyCollection<MetadataBlock> CompactMergeBlocks(
            IEnumerable<MetadataBlock> blocks,
            MetadataTableSchema schema,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var blockIdsToCompactSet = blockIdsToCompact.ToHashSet();
            var blockStack = new Stack<MetadataBlock>(blocks.OrderByDescending(b => b.MinRecordId));
            var processedBlocks = new List<MetadataBlock>(2 * blockStack.Count());
            var blockBuilder = new BlockBuilder(schema.ParentSchema);
            MetadataBlock? previousBlock = null;

            while (blockStack.Count > 0)
            {
                var currentBlock = blockStack.Pop();

                CompactMergeOneBlock(
                    currentBlock,
                    schema,
                    blockBuilder,
                    ref previousBlock,
                    processedBlocks,
                    blockIdsToCompactSet,
                    allTombstoneBlockIndex,
                    tx);
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
                schema,
                tx);

            return processedBlocks;
        }

        private void CompactMergeOneBlock(
           MetadataBlock currentBlock,
           MetadataTableSchema schema,
           BlockBuilder blockBuilder,
           ref MetadataBlock? previousBlock,
           List<MetadataBlock> processedBlocks,
           ISet<int> blockIdsToCompact,
           IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
           TransactionContext tx)
        {
            if (((IBlock)blockBuilder).RecordCount > 0 && previousBlock != null)
            {
                throw new InvalidOperationException("Both blocks can't exist at the same time");
            }

            if (allTombstoneBlockIndex.TryGetValue(currentBlock.BlockId, out var tb)
                && tb.RowIndexes.Count == currentBlock.ItemCount)
            {
                //  All good:  opportunistically (i.e. regardless if it is in blockIdsToCompact
                //  or not) discard an entirely deleted block
            }
            else if (blockIdsToCompact.Contains(currentBlock.BlockId))
            {   //  We try to compact regardless if the blockBuilder has records or not
                CompactIntoBuilder(currentBlock, blockBuilder, allTombstoneBlockIndex);
                //  Removing rows increases block size (rare)
                PersistBlockBuilder(
                    blockBuilder,
                    false,
                    processedBlocks,
                    schema,
                    tx);
                if (previousBlock != null)
                {
                    //  Try to merge previous block with compacted block
                    if (((IBlock)blockBuilder).RecordCount > 0 && TryMerge(
                        blockBuilder,
                        previousBlock,
                        allTombstoneBlockIndex,
                        tx))
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
                tx))
            {
                //  All good
            }
            else if (previousBlock != null && TryMerge(
                blockBuilder,
                currentBlock,
                previousBlock,
                allTombstoneBlockIndex,
                tx))
            {
                previousBlock = null;
            }
            else
            {   //  Persist the builder completly
                PersistBlockBuilder(
                    blockBuilder,
                    true,
                    processedBlocks,
                    schema,
                    tx);
                if (previousBlock != null)
                {
                    processedBlocks.Add(previousBlock);
                }
                previousBlock = currentBlock;
            }
        }

        private void CompactIntoBuilder(
            MetadataBlock currentBlock,
            BlockBuilder blockBuilder,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex)
        {
            var schema = ((IBlock)blockBuilder).TableSchema;

            if (!schema.IsMetadata
                && allTombstoneBlockIndex.TryGetValue(currentBlock.BlockId, out var tb))
            {   //  Some tombstones
                var recordCountBefore = ((IBlock)blockBuilder).RecordCount;

                if (tb.RowIndexes.Count != currentBlock.ItemCount)
                {   //  Partial delete
                    var tombstoneRowIndexes = tb
                        .RowIndexes
                        .OrderBy(x => x)
                        .Select(x => x + recordCountBefore)
                        .ToArray();
                    var block = Database.GetOrLoadBlock(currentBlock.BlockId, schema);

                    blockBuilder.AppendBlock(block);
#if DEBUG
                    var dataSchema = (DataTableSchema)schema;
                    var deletedRecordIds = ((IBlock)blockBuilder).Project(
                        new object?[1],
                        [dataSchema.RecordIdColumnIndex],
                        tombstoneRowIndexes)
                        .Select(r => (long)r.Span[0]!)
                        .ToHashSet();

                    if (!deletedRecordIds.SetEquals(tb.RecordIds))
                    {
                        throw new InvalidOperationException("Inconsistent tombstone records");
                    }
#endif
                    blockBuilder.DeleteRecordsByRecordIndex(tombstoneRowIndexes);
#if DEBUG
                    if (recordCountBefore + currentBlock.ItemCount - tombstoneRowIndexes.Length
                        != ((IBlock)blockBuilder).RecordCount)
                    {
                        throw new InvalidOperationException("Compaction error");
                    }
#endif
                }
            }
            else
            {   //  No tombstones
                var block = Database.GetOrLoadBlock(currentBlock.BlockId, schema);

                blockBuilder.AppendBlock(block);
            }
        }

        private bool TryMerge(
            BlockBuilder blockBuilder,
            MetadataBlock nextMetadataBlock,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
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

                CompactIntoBuilder(nextMetadataBlock, blockBuilder, allTombstoneBlockIndex);
                if (blockBuilder.GetSerializationSize() <= _maxBlockSize)
                {
                    return true;
                }
                else
                {   //  Roll back append
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

        private bool TryMerge(
            BlockBuilder blockBuilder,
            MetadataBlock metadataBlockA,
            MetadataBlock metadataBlockB,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
#if DEBUG
            if (((IBlock)blockBuilder).RecordCount > 0)
            {
                throw new InvalidOperationException("Block builder should be empty here");
            }
            if (Database.GetMetaDataTable(((IBlock)blockBuilder).TableSchema.TableName).Schema.TableName
                != metadataBlockA.Schema.TableName)
            {
                throw new InvalidOperationException("Inconsistant schema");
            }
            if (metadataBlockA.Schema.TableName != metadataBlockB.Schema.TableName)
            {
                throw new InvalidOperationException("Inconsistant schema");
            }
#endif

            var totalSize = metadataBlockA.Size + metadataBlockB.Size;

            if (totalSize <= _maxBlockSize)
            {
                CompactIntoBuilder(metadataBlockA, blockBuilder, allTombstoneBlockIndex);
                CompactIntoBuilder(metadataBlockB, blockBuilder, allTombstoneBlockIndex);
                if (blockBuilder.GetSerializationSize() <= _maxBlockSize)
                {
                    return true;
                }
                else
                {   //  Roll back compact
                    blockBuilder.Clear();

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
            MetadataTableSchema schema,
            TransactionContext tx)
        {
            if (((IBlock)blockBuilder).RecordCount > 0)
            {
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
                        var metadataRecord = schema.CreateMetadataRecord(blockId, blockStats);

                        if (blockStats.Size != size.Size)
                        {
                            throw new InvalidOperationException(
                                $"Mismatch between predicted size ({size.Size}) and serialized" +
                                $"size ({blockStats.Size})");
                        }
                        Database.PersistBlock(blockId, actualBuffer, tx);
                        processedBlocks.Add(new MetadataBlock(metadataRecord, schema));
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

        private IEnumerable<MetadataBlock> PersistMetaBlocks(
            int? oldMetaBlockId,
            MetadataTableSchema schema,
            IReadOnlyCollection<MetadataBlock> processedBlocks,
            TransactionContext tx)
        {
            if (processedBlocks.Count == 0)
            {
                if (oldMetaBlockId == null)
                {   //  Ensure we clear the root
                    GetCleanMetaBlockBuilder(schema, tx);
                }

                return Array.Empty<MetadataBlock>();
            }
            else
            {
                var blockBuilder = oldMetaBlockId == null
                    ? GetCleanMetaBlockBuilder(schema, tx)
                    : new BlockBuilder(schema, processedBlocks.Count);

                foreach (var record in processedBlocks.Select(b => b.MetadataRecord))
                {
                    blockBuilder.AppendRecord(record.Span);
                }
                if (oldMetaBlockId != null)
                {   //  If non-null meta block ID, we persist into a new block
                    var metaTable = Database.GetMetaDataTable(schema.TableName);
                    var metaSchema = (MetadataTableSchema)metaTable.Schema;
                    var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
                    var metaBlocks = new List<MetadataBlock>(2);
                    var sizes = blockBuilder.SegmentRecords(buffer.Length);
                    var newBlockIds = Database.AvailabilityBlockManager.SetInUse(sizes.Count, tx);
                    var skipRows = 0;

                    foreach (var pair in sizes.Zip(newBlockIds))
                    {
                        var size = pair.First;
                        var newBlockId = pair.Second;
                        var blockStats = blockBuilder.Serialize(buffer, skipRows, size.ItemCount);
                        var metaRecord = metaSchema.CreateMetadataRecord(newBlockId, blockStats);
                        var metaBlock = new MetadataBlock(metaRecord, metaSchema);

                        Database.PersistBlock(newBlockId, buffer, tx);
                        metaBlocks.Add(metaBlock);
                        skipRows += size.ItemCount;
                    }

                    return metaBlocks;
                }
                else
                {   //  Otherwise, we already replaced inplace
                    return Array.Empty<MetadataBlock>();
                }
            }
        }

        private BlockBuilder GetCleanMetaBlockBuilder(
            MetadataTableSchema metadataSchema,
            TransactionContext tx)
        {
            var tableName = metadataSchema.TableName;

            tx.LoadCommittedBlocksInTransaction(tableName);

            var tableLog = tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[tableName];

            tableLog.NewDataBlock.Clear();
            tableLog.CommittedDataBlock!.Clear();

            return tableLog.CommittedDataBlock!;
        }

        #region Load Blocks
        private IEnumerable<MetadataBlock> LoadBlocks(
            string tableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            if (metaBlockId != null && metaBlockId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(metaBlockId));
            }

            var table = Database.GetAnyTable(tableName);
            var schema = (MetadataTableSchema)table.Schema;
            var columnIndexes = Enumerable.Range(0, schema.Columns.Count)
                .ToImmutableArray();

            IEnumerable<ReadOnlyMemory<object?>> LoadNullBlocks(string tableName)
            {
                var results = table.Query(tx)
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithInMemoryOnly()
                    .WithProjection(columnIndexes);

                return results;
            }

            IEnumerable<ReadOnlyMemory<object?>> LoadPositiveBlocks(
                string tableName,
                int metaBlockId)
            {
                var metaMetaBlock = Database.GetOrLoadBlock(metaBlockId, schema);
                var results = metaMetaBlock.Project(
                    new object?[columnIndexes.Length],
                    columnIndexes,
                    Enumerable.Range(0, metaMetaBlock.RecordCount));

                return results;
            }

            var results = metaBlockId == null
                ? LoadNullBlocks(tableName)
                : LoadPositiveBlocks(tableName, metaBlockId.Value);
            var blocks = results
                .Select(r => new MetadataBlock(r.ToArray(), schema))
                .ToImmutableArray();

            return blocks;
        }
        #endregion
    }
}
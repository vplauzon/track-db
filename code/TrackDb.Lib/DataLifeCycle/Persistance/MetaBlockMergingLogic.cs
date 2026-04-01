using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
            TableSchema schema,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            IDictionary<int, IEnumerable<MetadataBlock>> blockReplacementMap,
            TransactionContext tx)
        {
            var metadataTable = Database.GetMetaDataTable(schema.TableName);
            var metadataSchema = (MetadataTableSchema)metadataTable.Schema;
            var blocks = LoadBlocks(schema.TableName, metaBlockId, tx);
            var replacedBlocks = blocks
                .SelectMany(b => blockReplacementMap.TryGetValue(b.BlockId, out var replacements)
                ? replacements
                : [b])
                .ToArray();
            var processedBlocks = CompactMergeBlocks(
                replacedBlocks,
                schema,
                metadataSchema,
                blockIdsToCompact,
                allTombstoneBlockIndex,
                tx);
            var deletedBlockIds = replacedBlocks
                .Select(b => b.BlockId)
                .Except(processedBlocks.Select(b => b.BlockId))
                .ToArray();
            var metaMetadataBlocks = PersistMetaBlocks(metaBlockId, metadataSchema, processedBlocks, tx);

            return new(deletedBlockIds, metaMetadataBlocks);
        }

        private IReadOnlyCollection<MetadataBlock> CompactMergeBlocks(
            IEnumerable<MetadataBlock> blocks,
            TableSchema schema,
            MetadataTableSchema metadataSchema,
            IEnumerable<int> blockIdsToCompact,
            IDictionary<int, TombstoneBlock> allTombstoneBlockIndex,
            TransactionContext tx)
        {
            var blockIdsToCompactSet = blockIdsToCompact.ToHashSet();
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
                metadataSchema,
                tx);

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
           TransactionContext tx)
        {
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
                    tx);
                //  Removing rows increases block size (rare)
                PersistBlockBuilder(
                    blockBuilder,
                    false,
                    processedBlocks,
                    metaSchema,
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
            else
            {   //  Persist the builder completly
                PersistBlockBuilder(
                    blockBuilder,
                    true,
                    processedBlocks,
                    metaSchema,
                    tx);
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
            TransactionContext tx)
        {
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
            TransactionContext tx)
        {
            if (((IBlock)blockBuilder).RecordCount > 0)
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

        private IEnumerable<MetadataBlock> PersistMetaBlocks(
            int? oldMetaBlockId,
            MetadataTableSchema metadataSchema,
            IReadOnlyCollection<MetadataBlock> processedBlocks,
            TransactionContext tx)
        {
            if (processedBlocks.Count == 0)
            {
                return Array.Empty<MetadataBlock>();
            }
            else
            {
                var metaMetaBlockBuilder = oldMetaBlockId == null
                    ? GetCleanMetaBlockBuilder(metadataSchema, tx)
                    : new BlockBuilder(metadataSchema, processedBlocks.Count);
                var metaTable = Database.GetAnyTable(metadataSchema.TableName);
                var metaMetaTable = Database.GetMetaDataTable(metadataSchema.TableName);
                var metaMetaSchema = (MetadataTableSchema)metaMetaTable.Schema;
                var recordIds = metaTable.NewRecordIds(processedBlocks.Count);
                var pairs = processedBlocks
                    .Select(b => b.MetadataRecord)
                    .Zip(recordIds, (r, id) => new { Record = r, RecordId = id });

                foreach (var pair in pairs)
                {
                    metaMetaBlockBuilder.AppendRecord(pair.RecordId, pair.Record.Span);
                }

                if (oldMetaBlockId != null)
                {
                    var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
                    var metaMetadataBlocks = new List<MetadataBlock>(2);
                    var sizes = metaMetaBlockBuilder.SegmentRecords(buffer.Length);
                    var newBlockIds = Database.AvailabilityBlockManager.SetInUse(sizes.Count, tx);
                    var skipRows = 0;

                    foreach (var pair in sizes.Zip(newBlockIds))
                    {
                        var size = pair.First;
                        var newBlockId = pair.Second;
                        var blockStats = metaMetaBlockBuilder.Serialize(buffer, skipRows, size.ItemCount);
                        var metaMetadataRecord = metaMetaSchema.CreateMetadataRecord(newBlockId, blockStats);
                        var metaMetadataBlock = new MetadataBlock(metaMetadataRecord, metaMetaSchema);

                        Database.PersistBlock(newBlockId, buffer, tx);
                        metaMetadataBlocks.Add(metaMetadataBlock);
                    }

                    return metaMetadataBlocks;
                }
                else
                {
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

            Table metaDataTable = Database.GetMetaDataTable(tableName);
            Table metaMetaDataTable = Database.GetMetaDataTable(metaDataTable.Schema.TableName);
            var metadataTableSchema = (MetadataTableSchema)metaDataTable.Schema;
            var columnIndexes = Enumerable.Range(0, metadataTableSchema.Columns.Count)
                .ToImmutableArray();

            IEnumerable<ReadOnlyMemory<object?>> LoadNullBlocks(string tableName)
            {
                var results = metaDataTable.Query(tx)
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
                var metaMetaBlock = Database.GetOrLoadBlock(metaBlockId, metaDataTable.Schema);
                var results = metaMetaBlock.Project(
                    new object?[columnIndexes.Length],
                    columnIndexes,
                    Enumerable.Range(0, metaMetaBlock.RecordCount),
                    0);

                return results;
            }

            var results = metaBlockId == null
                    ? LoadNullBlocks(tableName)
                    : LoadPositiveBlocks(tableName, metaBlockId.Value);
            var blocks = results
                .Select(r => new MetadataBlock(r.ToArray(), metadataTableSchema))
                .ToImmutableArray();

            return blocks;
        }
        #endregion
    }
}
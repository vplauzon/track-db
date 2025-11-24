using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record BlockInfo(
            //  Metadata only
            MetaDataBlock? MetaDataBlock,
            //  Signals the metadata is new and not persisted to its meta block yet
            bool IsNew,
            //  Data only
            BlockBuilder? DataBlockBuilder)
        {
            #region Constructors
            public static BlockInfo FromMetadataBlock(MetaDataBlock metaDataBlock)
            {
                return new BlockInfo(metaDataBlock, false, null);
            }

            public static BlockInfo FromBlockId(
                int blockId,
                Database database,
                TableSchema schema,
                TransactionContext tx)
            {
                void Compact(BlockBuilder blockBuilder)
                {   //  Find tombstoned records
                    var allDeletedRecordIds = database.TombstoneTable.Query(tx)
                        .Where(pf => pf.Equal(t => t.TableName, schema.TableName))
                        .Select(t => t.DeletedRecordId)
                        .ToImmutableArray();
                    //  Hard delete the ones in the block
                    var hardDeletedRecordIds =
                        blockBuilder.DeleteRecordsByRecordId(allDeletedRecordIds);

                    //  Remove those records from tombstone table
                    tx.LoadCommittedBlocksInTransaction(database.TombstoneTable.Schema.TableName);

                    if (hardDeletedRecordIds.Any())
                    {
                        var predicate = database.TombstoneTable.Query(tx)
                                            .Where(pf => pf.Equal(t => t.TableName, schema.TableName))
                                            .Where(pf => pf.In(t => t.DeletedRecordId, hardDeletedRecordIds))
                                            .Predicate;
                        var tombstoneBuilder = tx.TransactionState.UncommittedTransactionLog
                            .TransactionTableLogMap[database.TombstoneTable.Schema.TableName]
                            .CommittedDataBlock!;
                        var hardDeleteRowIndexes = ((IBlock)tombstoneBuilder)
                            .Filter(predicate, false)
                            .RowIndexes;

                        tombstoneBuilder.DeleteRecordsByRecordIndex(hardDeleteRowIndexes);
                    }
                }

                var block = database.GetOrLoadBlock(blockId, schema);
                var blockBuilder = new BlockBuilder(schema);

                blockBuilder.AppendBlock(block);
                Compact(blockBuilder);

                return new BlockInfo(null, false, blockBuilder);
            }
            #endregion

            public long ComputeRecordIdMax()
            {
                if (MetaDataBlock != null)
                {
                    return MetaDataBlock.RecordIdMax;
                }
                else
                {
                    var block = ((IBlock)DataBlockBuilder!);
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

            public int ComputeSize()
            {
                if (MetaDataBlock != null)
                {
                    return MetaDataBlock.Size;
                }
                else
                {
                    var blockStats = DataBlockBuilder!.Serialize(Array.Empty<byte>());

                    return blockStats.Size;
                }
            }

            public BlockInfo Persist(Database database, MetadataTableSchema metaSchema)
            {
                if (DataBlockBuilder == null || ((IBlock)DataBlockBuilder).RecordCount == 0)
                {
                    throw new InvalidOperationException();
                }
                var buffer = new byte[database.DatabasePolicy.StoragePolicy.BlockSize];
                var blockStats = DataBlockBuilder.Serialize(buffer);

                if (blockStats.Size > buffer.Length)
                {
                    throw new InvalidOperationException("Block bigger than planned");
                }

                using (var tx = database.CreateTransaction())
                {
                    var blockId = database.PersistBlock(buffer.AsSpan().Slice(0, blockStats.Size), tx);
                    var metaRecord = metaSchema.CreateMetadataRecord(blockId, blockStats);
                    var metaBlock = new MetaDataBlock(metaRecord, metaSchema);

                    tx.Complete();

                    return new BlockInfo(metaBlock, true, null);
                }
            }

            public BlockInfo? Merge(BlockInfo rightBlock, int maxBlockSize)
            {
                var leftBuilder = DataBlockBuilder!;
                var rightBuilder = rightBlock.DataBlockBuilder!;
                var mergedBuilder = new BlockBuilder(((IBlock)leftBuilder).TableSchema);

                mergedBuilder.AppendBlock(leftBuilder);
                mergedBuilder.AppendBlock(rightBuilder);

                if (mergedBuilder.Serialize(Array.Empty<byte>()).Size <= maxBlockSize)
                {
                    return new BlockInfo(null, false, mergedBuilder);
                }
                else
                {
                    return null;
                }
            }
        }

        private record BlockReplacement(int BlockId, BlockInfo BlockInfo);
        #endregion

        public BlockMergingAgentBase(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Compact (remove deleted records) blocks from a data table (generation 1) and merge them
        /// together, updating blocks "in place".
        /// </summary>
        /// <param name="dataTableName"></param>
        /// <param name="deletedRecordId">Starting record ID in the table.</param>
        /// <param name="tx"></param>
        protected void CompactBlock(string dataTableName, long deletedRecordId, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var schema = tableMap[dataTableName].Table.Schema;
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
                tx.LoadCommittedBlocksInTransaction(Database.TombstoneTable.Schema.TableName);

                var tombstoneBuilder = tx.TransactionState
                    .UncommittedTransactionLog
                    .TransactionTableLogMap[Database.TombstoneTable.Schema.TableName]
                    .CommittedDataBlock!;
                var predicate = Database.TombstoneTable.Query(tx)
                    .Where(pf => pf.Equal(t => t.TableName, dataTableName))
                    .Where(pf => pf.Equal(t => t.DeletedRecordId, deletedRecordId))
                    .Predicate;
                var rowIndexes = ((IBlock)tombstoneBuilder).Filter(predicate, false).RowIndexes;

                tombstoneBuilder.DeleteRecordsByRecordIndex(rowIndexes);
            }
            else
            {
                var blockInfo = BlockInfo.FromBlockId(
                    deletedRecordBlockId.Value,
                    Database,
                    schema,
                    tx);
                var parentBlockId = FindParentBlock(metadataTableName, deletedRecordBlockId.Value, tx);

                MergeSubBlocksWithReplacements(
                    metadataTableName,
                    parentBlockId,
                    [new BlockReplacement(deletedRecordBlockId.Value, blockInfo)],
                    tx);
            }
        }

        /// <summary>
        /// Merges all children blocks of <paramref name="metaMetaBlockId"/>.
        /// </summary>
        /// <param name="metadataTableName">
        /// Metadata table where the blocks (not <paramref name="metaMetaBlockId"/>) live
        /// </param>
        /// <param name="metaMetaBlockId"></param>
        /// <param name="tx"></param>
        protected void MergeSubBlocks(
            string metadataTableName,
            int? metaMetaBlockId,
            TransactionContext tx)
        {
            MergeSubBlocksWithReplacements(
                metadataTableName,
                metaMetaBlockId,
                Array.Empty<BlockReplacement>(),
                tx);
        }

        #region Merge algorithm
        private void MergeSubBlocksWithReplacements(
            string metadataTableName,
            int? metaMetaBlockId,
            IEnumerable<BlockReplacement> replacements,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var blockInfoStack = new Stack<BlockInfo>(
                LoadBlockInfos(metadataTableName, metaMetaBlockId, replacements, tx)
                .Where(b => b.MetaDataBlock != null || ((IBlock)b.DataBlockBuilder!).RecordCount > 0)
                .Select(b => new
                {
                    BlockInfo = b,
                    RecordIdMax = b.ComputeRecordIdMax()
                })
                .OrderBy(o => o.RecordIdMax)
                .Select(o => o.BlockInfo));
            var processedBlockInfos = new List<BlockInfo>(blockInfoStack.Count);
            var metaSchema = (MetadataTableSchema)tableMap[metadataTableName].Table.Schema;
            var schema = metaSchema.ParentSchema;

            MergeBlockStack(blockInfoStack, processedBlockInfos, metaSchema, tx);
            tx.LoadCommittedBlocksInTransaction(metadataTableName);
            if (metaMetaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                PostMergeInMemoryMetaBlock(metadataTableName, processedBlockInfos, tx);
            }
        }

        private void PostMergeInMemoryMetaBlock(
            string metadataTableName,
            List<BlockInfo> processedBlockInfos,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var metaBlockBuilder = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[metadataTableName]
                .CommittedDataBlock!;

            //  Delete blocks in-memory
            metaBlockBuilder.DeleteRecordsByRecordIndex(
                Enumerable.Range(0, ((IBlock)metaBlockBuilder).RecordCount));
            if (processedBlockInfos.Any())
            {   //  Insert meta records
                var metaTable = tableMap[metadataTableName].Table;

                foreach (var blockInfo in processedBlockInfos)
                {
                    metaBlockBuilder.AppendRecord(
                        DateTime.Now,
                        metaTable.NewRecordId(),
                        blockInfo.MetaDataBlock!.MetadataRecord.Span);
                }
            }
        }

        private void MergeBlockStack(
            Stack<BlockInfo> blockInfoStack,
            List<BlockInfo> processedBlockInfos,
            MetadataTableSchema metaSchema,
            TransactionContext tx)
        {
            var maxBlockSize = Database.DatabasePolicy.StoragePolicy.BlockSize;

            while (blockInfoStack.Any())
            {
                var leftBlock = blockInfoStack.Pop();

                if (blockInfoStack.Any())
                {
                    var rightBlock = blockInfoStack.Pop();

                    if (leftBlock.ComputeSize() + rightBlock.ComputeSize() <= maxBlockSize)
                    {
                        leftBlock = leftBlock.DataBlockBuilder != null
                            ? leftBlock
                            : BlockInfo.FromBlockId(
                                leftBlock.MetaDataBlock!.BlockId,
                                Database,
                                metaSchema.ParentSchema,
                                tx);
                        rightBlock = rightBlock.DataBlockBuilder != null
                            ? rightBlock
                            : BlockInfo.FromBlockId(
                                rightBlock.MetaDataBlock!.BlockId,
                                Database,
                                metaSchema.ParentSchema,
                                tx);

                        var mergedBlock = leftBlock.Merge(rightBlock, maxBlockSize);

                        if (mergedBlock != null)
                        {
                            blockInfoStack.Push(mergedBlock);
                        }
                        else
                        {
                            processedBlockInfos.Add(leftBlock.Persist(Database, metaSchema));
                            blockInfoStack.Push(rightBlock);
                        }
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }
                else
                {   //  We're done
                    if (leftBlock.DataBlockBuilder != null)
                    {
                        if (((IBlock)leftBlock.DataBlockBuilder).RecordCount > 0)
                        {
                            processedBlockInfos.Add(leftBlock.Persist(Database, metaSchema));
                        }
                    }
                    else
                    {
                        processedBlockInfos.Add(leftBlock);
                    }
                }
            }
        }
        #endregion

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

        #region Load block infos
        private IImmutableList<BlockInfo> LoadBlockInfos(
            string metadataTableName,
            int? metaBlockId,
            IEnumerable<BlockReplacement> replacements,
            TransactionContext tx)
        {
            if (metaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {   //  We merge blocks in memory
                tx.LoadCommittedBlocksInTransaction(metadataTableName);

                var replacementMap = replacements
                    .ToImmutableDictionary(r => r.BlockId, r => r.BlockInfo);
                var blockInfos = tx.TransactionState.InMemoryDatabase
                    .TransactionTableLogsMap[metadataTableName]
                    .InMemoryBlocks
                    .SelectMany(b => LoadBlockInfosFromMetaBlock(b))
                    .Select(b => replacementMap.ContainsKey(b.MetaDataBlock!.BlockId)
                    ? replacementMap[b.MetaDataBlock!.BlockId]
                    : b)
                    .ToImmutableArray();

                return blockInfos;
            }
        }

        private IEnumerable<BlockInfo> LoadBlockInfosFromMetaBlock(IBlock metaBlock)
        {
            return metaBlock.Project(
                new object?[metaBlock.TableSchema.Columns.Count],
                Enumerable.Range(0, metaBlock.TableSchema.Columns.Count)
                .ToImmutableArray(),
                Enumerable.Range(0, metaBlock.RecordCount),
                0)
                .Select(r => BlockInfo.FromMetadataBlock(
                    new MetaDataBlock(r.ToArray(), (MetadataTableSchema)metaBlock.TableSchema)));
        }
        #endregion
    }
}